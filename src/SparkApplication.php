<?php


namespace Keboola\PythonSparkTransformation;

use Closure;
use DateTime;
use GuzzleHttp\Client as GuzzleClient;
use GuzzleHttp\Exception\GuzzleException;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\MessageFormatter;
use GuzzleHttp\Middleware;
use GuzzleHttp\Psr7\Request;
use JsonException;
use Keboola\Component\Logger;
use Keboola\PythonSparkTransformation\Configuration\Config;
use Keboola\PythonSparkTransformation\Exception\ApplicationException;
use Keboola\PythonSparkTransformation\Exception\UserException;
use Keboola\PythonSparkTransformation\Transformation\Config\Code;
use MicrosoftAzure\Storage\Blob\BlobRestProxy;
use MicrosoftAzure\Storage\Blob\BlobSharedAccessSignatureHelper;
use MicrosoftAzure\Storage\Common\Internal\Resources;
use Psr\Http\Message\RequestInterface;
use Psr\Http\Message\ResponseInterface;
use Psr\Log\LoggerInterface;

class SparkApplication
{
    private const DEFAULT_USER_AGENT = 'Internal DataMechanics API PHP Client';
    private const DEFAULT_BACKOFF_RETRIES = 10;
    private const JSON_DEPTH = 512;

    private const SAS_WRITE_CONTAINER_PRIVILEGE = 'rwl';
    private const SAS_READ_CONTAINER_PRIVILEGE = 'rl';
    private const SAS_DEFAULT_EXPIRATION_HOURS = 12;

    private string $dataMechanicsUrl;

    private string $dataMechanicsToken;

    private string $absConnectionString;

    private string $configurationTemplate;

    private Logger $logger;

    private string $appName;

    private string $jobName;

    private Config $config;

    private BlobRestProxy $blobClient;

    private BlobSharedAccessSignatureHelper $sasHelper;

    public function __construct(Config $config, Logger $logger)
    {
        $imageParameters = $config->getImageParameters();
        $this->dataMechanicsUrl = $imageParameters['dataMechanicsUrl'];
        $this->dataMechanicsToken = $imageParameters['#dataMechanicsToken'];
        $this->configurationTemplate = $imageParameters['configurationTemplate'];
        $this->blobClient = BlobRestProxy::createBlobService(
            $config->getParameters()['#absConnectionString']
        );
        $this->sasHelper = new BlobSharedAccessSignatureHelper(
            $imageParameters['absAccountName'],
            $imageParameters['absAccountKey']
        );
        $this->logger = $logger;
        $this->config = $config;
    }

    public function setAppName(string $appName): self
    {
        $this->appName = $appName;
        return $this;
    }

    public function getAppName(): string
    {
        return $this->appName;
    }

    public function getFileName(): string
    {
        return $this->appName . 'py';
    }

    public function setJobName(string $jobName): self
    {
        $this->jobName = $jobName;
        return $this;
    }

    public function getJobName(): string
    {
        return $this->jobName;
    }

    public function packageScript(): void
    {
        $blocks = $this->config->getBlocks();
        $script = '';
        foreach ($blocks as $block) {
            foreach ($block->getCodes() as $code) {
                /** @var Code $code*/
                $script .= implode("\n", $code->getScripts());
            }
        }
        $this->blobClient->createBlockBlob(
            getenv('ABS_CONTAINER'),
            $this->getFileName(),
            $script
        );
    }

    private function generateLinkToScript(): string
    {
        $expirationDate = (new DateTime())->modify('+' . self::SAS_DEFAULT_EXPIRATION_HOURS . 'hour');
        $sas = $this->sasHelper->generateBlobServiceSharedAccessSignatureToken(
            Resources::RESOURCE_TYPE_CONTAINER,
            getenv('ABS_CONTAINER'),
            self::SAS_WRITE_CONTAINER_PRIVILEGE,
            $expirationDate,
            (new DateTime())
        );
        $sasConnectionString = sprintf(
            '%s=https://%s.%s;%s=%s',
            Resources::BLOB_ENDPOINT_NAME,
            $this->blobClient->getAccountName(),
            Resources::BLOB_BASE_DNS_NAME,
            Resources::SAS_TOKEN_NAME,
            $sas
        );

        $blobClientWithSAS = BlobRestProxy::createBlobService(
            $sasConnectionString
        );

        return sprintf(
            '%s%s?%s',
            (string) $blobClientWithSAS->getPsrPrimaryUri(),
            $this->getFileName(),
            $sas
        );
    }

    public function run(): void
    {
        $scriptlink = $this->generateLinkToScript();
        $dmClient = $this->initClient([
            'apiUrl' => $this->dataMechanicsUrl . '/api/',
            'logger' => $this->logger
        ]);
        $jobData = json_encode([
            'appName' => $this->getAppName(),
            'jobName' => $this->getJobName(),
            'configTemplateName' => $this->configurationTemplate,
            'configOverrides' => [
                'mainApplicationFile' => $scriptlink,
            ],
        ]);
        $request = new Request('POST', 'apps', [], $jobData);
        try {
            $response = $dmClient->send($request);
            $data = json_decode($response->getBody()->getContents(), true, self::JSON_DEPTH, JSON_THROW_ON_ERROR);
            return $data ?: [];
        } catch (GuzzleException $e) {
            throw new UserException($e->getMessage(), $e->getCode(), $e);
        }
    }

    protected function initClient(array $options = []): GuzzleClient
    {
        // Initialize handlers (start with those supplied in constructor)
        if (isset($options['handler']) && $options['handler'] instanceof HandlerStack) {
            $handlerStack = HandlerStack::create($options['handler']);
        } else {
            $handlerStack = HandlerStack::create();
        }
        // Set exponential backoff
        $handlerStack->push(Middleware::retry($this->createDefaultDecider($options['backoffMaxTries'])));
        // Set handler to set default headers
        $handlerStack->push(Middleware::mapRequest(
            function (RequestInterface $request) use ($options) {
                $requestUpdated = $request
                    ->withHeader('User-Agent', $options['userAgent'])
                    ->withHeader('Content-type', 'application/json');
                foreach ($options['headers'] as $key => $value) {
                    $requestUpdated = $requestUpdated->withHeader($key, $value);
                }
                return $requestUpdated;
            }
        ));
        // Set client logger
        if (isset($options['logger']) && $options['logger'] instanceof LoggerInterface) {
            $handlerStack->push(Middleware::log(
                $options['logger'],
                new MessageFormatter('[sandboxes-api] {method} {uri} : {code} {res_header_Content-Length}')
            ));
        }
        // finally create the instance
        return new GuzzleClient(['base_uri' => $options['apiUrl'], 'handler' => $handlerStack]);
    }

    protected function createDefaultDecider(int $maxRetries): Closure
    {
        return function (
            $retries,
            RequestInterface $request,
            ?ResponseInterface $response = null,
            $error = null
        ) use ($maxRetries) {
            if ($retries >= $maxRetries) {
                return false;
            } elseif ($response && $response->getStatusCode() >= 500) {
                return true;
            } elseif ($error) {
                return true;
            } else {
                return false;
            }
        };
    }
}

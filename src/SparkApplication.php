<?php

declare(strict_types=1);

namespace Keboola\PythonSparkTransformation;

use Closure;
use DateTime;
use GuzzleHttp\Client as GuzzleClient;
use GuzzleHttp\Exception\GuzzleException;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\MessageFormatter;
use GuzzleHttp\Middleware;
use GuzzleHttp\Psr7\Request;
use Keboola\Component\Logger;
use Keboola\PythonSparkTransformation\Exception\UserException;
use MicrosoftAzure\Storage\Blob\BlobRestProxy;
use Psr\Http\Message\RequestInterface;
use Psr\Http\Message\ResponseInterface;
use Psr\Log\LoggerInterface;

class SparkApplication
{
    private string $dataMechanicsUrl;

    private string $dataMechanicsToken;

    private string $sasConnectionString;

    private string $configurationTemplate;

    private Logger $logger;

    private string $appName;

    private string $jobName;

    private array $configParameters;

    private BlobRestProxy $blobClient;

    private string $sas;

    public function __construct(array $configParameters, array $imageParameters, Logger $logger)
    {
        var_dump($imageParameters);
        $this->dataMechanicsUrl = $imageParameters['dataMechanicsUrl'];
        $this->dataMechanicsToken = $imageParameters['#dataMechanicsToken'];
        $this->configurationTemplate = $imageParameters['configurationTemplate'];
        $this->sasConnectionString = $imageParameters['#sasConnectionString'];
        $this->absContainer = $imageParameters['absContainer'];
        $this->sas = $imageParameters['#sas'];
        $this->blobClient = BlobRestProxy::createBlobService(
            $this->sasConnectionString
        );
        $this->logger = $logger;
        $this->configParameters = $configParameters;
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
        return $this->appName . '.py';
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
        $blocks = $this->configParameters['blocks'];
        $script = '';
        foreach ($blocks as $block) {
            foreach ($block['codes'] as $code) {
                $script .= implode("\n", $code['script']);
            }
        }
        $this->blobClient->createBlockBlob(
            $this->absContainer,
            $this->getFileName(),
            $script
        );
    }

    private function generateLinkToScript(): string
    {
        return sprintf(
            '%s%s/%s?%s',
            str_replace('https', 'wasbs', (string) $this->blobClient->getPsrPrimaryUri()),
            $this->absContainer,
            $this->getFileName(),
            $this->sas
        );
    }

    public function run(): void
    {
        $scriptlink = $this->generateLinkToScript();
        $dmClient = new DataMechanicsClient(
            $this->dataMechanicsUrl,
            $this->dataMechanicsToken,
            $this->logger
        );
        $jobData = [
            'appName' => $this->getAppName(),
            'jobName' => $this->getJobName(),
            'configTemplateName' => $this->configurationTemplate,
            'configOverrides' => [
                'type' => 'Python',
                'mainApplicationFile' => $scriptlink,
                'sparkConf' => [
                    'spark.hadoop.fs.azure.account.auth.type' => 'SAS',
                    'spark.hadoop.fs.azure.sas.token.provider.type' => 'com.microsoft.azure.servicebus.security.SharedAccessSignatureTokenProvider'
                ],
            ],
        ];
        $dmClient->createApp($jobData);
    }
}

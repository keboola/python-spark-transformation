<?php

declare(strict_types=1);

namespace Keboola\PythonSparkTransformation;

use MicrosoftAzure\Storage\Blob\BlobRestProxy;
use Psr\Log\LoggerInterface;

class SparkApplication
{
    private string $dataMechanicsUrl;

    private string $dataMechanicsToken;

    private string $sasConnectionString;

    private string $absContainer;

    private string $absAccountName;

    private string $configurationTemplate;

    private LoggerInterface $logger;

    private string $appName;

    private string $jobName;

    private array $configParameters;

    private BlobRestProxy $blobClient;

    private string $sas;

    public function __construct(array $configParameters, array $imageParameters, LoggerInterface $logger)
    {
        $this->dataMechanicsUrl = $imageParameters['dataMechanicsUrl'];
        $this->dataMechanicsToken = $imageParameters['#dataMechanicsToken'];
        $this->configurationTemplate = $imageParameters['configurationTemplate'];
        $this->sasConnectionString = $imageParameters['#sasConnectionString'];
        $this->absContainer = $imageParameters['absContainer'];
        $this->absAccountName = $imageParameters['absAccountName'];
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
            'wasbs://%s@%s.blob.core.windows.net/%s',
            $this->absContainer,
            $this->absAccountName,
            $this->getFileName()
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
        $sparkVar = sprintf(
            'spark.hadoop.fs.azure.sas.%s.%s.blob.core.windows.net',
            $this->absContainer,
            $this->absAccountName
        );
        $jobData = [
            'appName' => $this->getAppName(),
            'jobName' => $this->getJobName(),
            'configTemplateName' => $this->configurationTemplate,
            'configOverrides' => [
                'type' => 'Python',
                'mainApplicationFile' => $scriptlink,
                'sparkConf' => [
                    $sparkVar => $this->sas,
                ],
            ],
        ];
        $dmClient->createApp($jobData);

        $isProcessed = false;
        $tries = 0;
        // give it some base startup time before starting polling.
        sleep(5);
        while (!$isProcessed) {
            $appDetails = $dmClient->getAppDetails($this->getAppName());
            if ($appDetails['status']['isProcessed']) {
                break;
            }
            $tries++;
            $waitAmount = call_user_func_array($dmClient->appDetailsDelayMethod(), [$tries]);
            sleep($waitAmount);
        }
        $metricsMessage = "Job metrics:\n";
        foreach ($appDetails['metrics'] as $metric => $value) {
            $metricsMessage .= "$metric : $value\n";
        }
        $this->logger->info('Spark job started at ' . $appDetails['status']['startedAt'] . ' and ended at ' . $appDetails['status']['startedAt']);
        $this->logger->info($metricsMessage);
    }
}

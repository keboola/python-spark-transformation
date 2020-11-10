<?php

declare(strict_types=1);

namespace Keboola\PythonSparkTransformation;

use GuzzleHttp\Psr7\Response;
use MicrosoftAzure\Storage\Blob\BlobRestProxy;
use Psr\Log\LoggerInterface;

class SparkApplication
{
    public const SPARK_COMPLETED_STATE = 'COMPLETED';

    public const LOG_CHUNK_SIZE = '1024';

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

    private DataMechanicsClient  $dmClient;

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
        $this->dmClient = new DataMechanicsClient(
            $this->dataMechanicsUrl,
            $this->dataMechanicsToken,
            $this->logger
        );
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

    public function run(): void
    {
        // bundle the code into a script
        $this->packageScript();

        // create the DM application
        $appDetails = $this->createApplication();
        $this->logger->info("Registered spark application " . $this->appName);

        // wait for the application to finish
        $appDetails = $this->waitForCompletion();

        // announce that we're finished
        $metricsMessage = "Job metrics:\n";
        foreach ($appDetails['metrics'] as $metric => $value) {
            $metricsMessage .= "$metric : $value\n";
        }
        $this->logger->info('Spark job started at ' . $appDetails['status']['startedAt'] . ' and ended at ' . $appDetails['status']['startedAt']);
        $this->logger->info($metricsMessage);
    }

    private function packageScript(): void
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

    private function createApplication(): array
    {
        $scriptlink = $this->generateLinkToScript();
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
        return $this->dmClient->createApp($jobData);
    }

    private function waitForCompletion(): array
    {
        // creating another client without the logger because we don't wan't to
        // log every poll request, we'll just log at some intervals with the job status return
        $polingClient = new DataMechanicsClient($this->dataMechanicsUrl, $this->dataMechanicsToken);
        $isProcessed = false;
        $tries = 0;
        $processedLogs = false;
        // give it some base startup time before starting polling.
        sleep(5);
        $totalWaitTime = 0;
        while (!$isProcessed) {
            $appDetails = $polingClient->getAppDetails($this->getAppName());
            if ($appDetails['status']['state'] === self::SPARK_COMPLETED_STATE && !$processedLogs) {
                // the job log should be available at this point
                $this->processLogResponse(
                    $polingClient->getLiveLogs($this->appName)
                );
                $processedLogs = true;
                $this->logger->info("Job completed, cleaning up");
            }
            if ($appDetails['status']['isProcessed']) {
                $this->logger->info(json_encode($appDetails['status']));
                break;
            }
            $tries++;
            $this->logger->info(json_encode($appDetails['status']));
            // $dmClient->getLiveLogs($this->appName);
            $waitAmount = call_user_func_array($polingClient->appDetailsDelayMethod(), [$tries]);
            $totalWaitTime += $waitAmount;
            sleep($waitAmount);
        }
        return $appDetails;
    }

    private function processLogResponse(Response $response): void
    {
        while (!$response->getBody()->eof()) {
            $this->logger->info($response->getBody()->read(self::LOG_CHUNK_SIZE));
        }
    }
}

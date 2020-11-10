<?php

declare(strict_types=1);

namespace Keboola\PythonSparkTransformation\Tests\PhpUnit\Transformation;

use Keboola\Component\Logger;
use Keboola\PythonSparkTransformation\SparkApplication;
use \PHPUnit\Framework\TestCase;

class TransformationTest extends TestCase
{
    private function generateTestScript(int $loopMax = 10, int $sleepInterval = 3): array
    {
        $testScript = [
            "import time \n",
            "print('starting') \n",
        ];
        $totalTimeSlept = 0;
        for ($i = 0; $i <= $loopMax; $i++) {
            $testScript[] = "time.sleep(" . $sleepInterval . ")";
            $totalTimeSlept += $sleepInterval;
            $testScript[] = "print('I have now slept for a total of $totalTimeSlept seconds') \n";
        }
        $testScript[] = "print('Test transformation completed') \n";
        return $testScript;
    }

    public function testTransformation(): void
    {
        $imageParameters = [
            'dataMechanicsUrl' => getenv('DATA_MECHANICS_URL'),
            '#dataMechanicsToken' => getenv('DATA_MECHANICS_TOKEN'),
            '#sasConnectionString' => getenv('ABS_SAS_CONNECTION_STRING'),
            '#sas' => getenv('ABS_SAS'),
            'configurationTemplate' => getenv('DM_CONFIGURATION_TEMPLATE'),
            'absContainer' => getenv('ABS_CONTAINER'),
            'absAccountName' => getenv('ABS_ACCOUNT_NAME'),
        ];

        $configParameters = [
            'blocks' => [
                [
                    'name' => 'first block',
                    'codes' => [
                        [
                            'name' => 'first code',
                            'script' => $this->generateTestScript(),
                        ],
                    ],
                ],
            ],
        ];
        $runId = (string) rand(1, 1000);
        $app = new SparkApplication($configParameters, $imageParameters, new Logger());
        $app->setAppName('transformation-test-' . $runId);
        $app->setJobName('transformation-test-' . $runId);
        $app->run();
    }
}

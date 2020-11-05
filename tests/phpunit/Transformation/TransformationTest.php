<?php

declare(strict_types=1);

namespace Keboola\PythonSparkTransformation\Tests\PhpUnit\Transformation;

use Keboola\Component\Logger;
use Keboola\PythonSparkTransformation\Configuration\Config;
use Keboola\PythonSparkTransformation\Configuration\ConfigDefinition;
use Keboola\PythonSparkTransformation\SparkApplication;
use \PHPUnit\Framework\TestCase;

class TransformationTest extends TestCase
{
    public function testTransformation(): void
    {
        $imageParameters = [
            'dataMechanicsUrl' => getenv('DATA_MECHANICS_URL'),
            '#dataMechanicsToken' => getenv('DATA_MECHANICS_TOKEN'),
            '#sasConnectionString' => getenv('ABS_SAS_CONNECTION_STRING'),
            '#sas' => getenv('ABS_SAS'),
            'configurationTemplate' => getenv('DM_CONFIGURATION_TEMPLATE'),
            'absContainer' => getenv('ABS_CONTAINER'),
        ];
        $configParameters = [
            'blocks' => [
                [
                    'name' => 'first block',
                    'codes' => [
                        [
                            'name' => 'first code',
                            'script' => [
                                "print('hello world') \n",
                                "print('goodbye world') \n",
                            ],
                        ],
                    ],
                ],
            ]
        ];
        $runId = (string) rand(1,1000);
        $app = new SparkApplication($configParameters, $imageParameters, new Logger());
        $app->setAppName('helloworld-' . $runId);
        $app->setJobName('transformation-test-' . $runId);
        $app->packageScript();
        $app->run();
    }
}

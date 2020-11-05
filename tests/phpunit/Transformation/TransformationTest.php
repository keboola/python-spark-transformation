<?php

declare(strict_types=1);

namespace Keboola\PythonSparkTransformation\Tests\PhpUnit\Transformation;

use _HumbugBox5f65e914a905\Psr\Log\NullLogger;
use Keboola\Component\Logger;
use Keboola\PythonSparkTransformation\Configuration\Config;
use Keboola\PythonSparkTransformation\SparkApplication;
use PHPUnit\Framework\Constraint\TraversableContainsIdentical;
use \PHPUnit\Framework\TestCase;

class TransformationTest extends TestCase
{
    public function testTransformation(): void
    {
        $imageParameters = [
            'dataMechanicsUrl' => getenv('DATA_MECHANICS_URL'),
            '#dataMechanicsToken' => getenv('DATA_MECHANICS_TOKEN'),
            '#absConnectionString' => getenv('ABS_SAS_CONNECTION_STRING'),
            'configurationTemplate' => getenv('DM_CONFIGURATION_TEMPLATE'),
            'absContainer' => getenv('ABS_CONTAINER'),
            'absAccountName' => getenv('ABS_ACCOUNT_NAME'),
            '#absAccountKey' => getenv('ABS_ACCOUNT_KEY'),
        ];
        $configParameters = [
            'blocks' => [
                [
                    'name' => 'first block',
                    'codes' => [
                        [
                            'name' => 'first code',
                            'scripts' => [
                                "print('hello world') \n",
                                "print('goodbye world') \n",
                            ],
                        ],
                    ],
                ],
            ]
        ];
        $config = new Config([
            'imageParameters' => $imageParameters,
            'parameters' => $configParameters,
            'storage' => [],
        ]);

        $app = new SparkApplication($config, new Logger());
        $app->setAppName('helloworld');
        $app->setJobName('transformation-test');
        $app->packageScript();
        $app->run();
    }
}

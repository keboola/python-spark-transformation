<?php

declare(strict_types=1);

namespace Keboola\PythonSparkTransformation;

use Psr\Log\LoggerInterface;
use Keboola\Component\BaseComponent;
use Keboola\PythonSparkTransformation\Configuration\Config;
use Keboola\PythonSparkTransformation\Configuration\ConfigDefinition;

class Component extends BaseComponent
{
    public function __construct(LoggerInterface $logger)
    {
        parent::__construct($logger);
    }

    protected function run(): void
    {
        $config = $this->getConfig();
        $application = new SparkApplication(
            $config->getParameters(),
            $config->getImageParameters(),
            $this->getLogger()
        );
        $application->run();
    }

    public function getConfig(): Config
    {
        $config = parent::getConfig();
        assert($config instanceof Config);
        return $config;
    }

    protected function getConfigClass(): string
    {
        return Config::class;
    }

    protected function getConfigDefinitionClass(): string
    {
        return ConfigDefinition::class;
    }
}

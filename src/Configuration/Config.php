<?php

declare(strict_types=1);

namespace Keboola\PythonSparkTransformation\Configuration;

use Keboola\Component\Config\BaseConfig;

class Config extends BaseConfig
{
    public function getHost(): string
    {
        return $this->getValue(['authorization', 'workspace', 'host']);
    }

    public function getPort(): int
    {
        return $this->getValue(['authorization', 'workspace', 'port']);
    }

    public function getUser(): string
    {
        return $this->getValue(['authorization', 'workspace', 'user']);
    }
}

<?php

declare(strict_types=1);

namespace Keboola\PythonSparkTransformation\Configuration;

use Keboola\Component\Config\BaseConfig;
use Keboola\PythonSparkTransformation\Exception\ApplicationException;
use Keboola\PythonSparkTransformation\Transformation\Config\Block;

class Config extends BaseConfig
{
    public function getRunId(): string
    {
        $runId = getenv('KBC_RUNID');
        if (!$runId) {
            throw new ApplicationException('KBC_RUNID environment variable must be set');
        }
        return $runId;
    }

    public function getBlocks(): array
    {
        return array_map(
            fn(array $data) => new Block($data),
            $this->getValue(['parameters', 'blocks'])
        );
    }
}

<?php

declare(strict_types=1);

namespace Keboola\PythonSparkTransformation\Transformation\Config;

class Code
{
    private string $name;

    private array $scripts;

    public function __construct(array $data)
    {
        $this->name = $data['name'];
        $this->scripts = $data['script'];
    }

    public function getName(): string
    {
        return $this->name;
    }

    public function getScripts(): array
    {
        return $this->scripts;
    }
}

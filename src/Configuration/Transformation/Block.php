<?php

declare(strict_types=1);

namespace Keboola\PythonSparkTransformation\Transformation\Config;

class Block
{
    private string $name;

    private array $codes;

    public function __construct(array $data)
    {
        $this->name = $data['name'];
        $this->codes = array_map(fn(array $data) => new Code($data), $data['codes']);
    }

    public function getName(): string
    {
        return $this->name;
    }

    /**
     * @return Code[]
     */
    public function getCodes(): array
    {
        return $this->codes;
    }
}

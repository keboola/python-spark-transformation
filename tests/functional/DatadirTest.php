<?php

declare(strict_types=1);

namespace Keboola\PythonSparkTransformation\FunctionalTests;

use Keboola\DatadirTests\DatadirTestCase;
use Keboola\PythonSparkTransformation\Exception\UserException;

class DatadirTest extends DatadirTestCase
{
    private string $datamechanicsToken;

    private string $datamechanicsUrl;

    protected function setUp(): void
    {
        if (!getenv('DATA_MECHANICS_TOKEN') || !getenv('DATA_MECHANICS_URL')) {
            throw new UserException('Env vars DATA_MECHANICS_TOKEN and DATA_MECHANICS_URL are required');
        }
        $this->datamechanicsToken = (string) getenv('DATA_MECHANICS_TOKEN');
        $this->datamechanicsUrl = (string) getenv('DATA_MECHANICS_URL');
        parent::setUp();
    }


}

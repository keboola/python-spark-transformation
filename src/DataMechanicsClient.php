<?php

declare(strict_types=1);

namespace Keboola\PythonSparkTransformation;

use Closure;
use GuzzleHttp\Client;
use GuzzleHttp\Client as GuzzleClient;
use GuzzleHttp\Exception\GuzzleException;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\MessageFormatter;
use GuzzleHttp\Middleware;
use GuzzleHttp\Psr7\Request;
use Keboola\PythonSparkTransformation\Exception\UserException;
use Psr\Http\Message\RequestInterface;
use Psr\Http\Message\ResponseInterface;
use Psr\Log\LoggerInterface;

class DataMechanicsClient
{
    private const DEFAULT_USER_AGENT = 'Internal DataMechanics API PHP Client';
    private const DEFAULT_BACKOFF_RETRIES = 1;
    private const JSON_DEPTH = 512;

    private Client $client;

    public function __construct(
        string $dataMechanicsUrl,
        string $dataMechanicsToken,
        LoggerInterface $logger
    ) {
        $this->client = $this->initClient([
            'apiUrl' => $dataMechanicsUrl . '/api/',
            'logger' => $logger,
            'backoffMaxTries' => self::DEFAULT_BACKOFF_RETRIES,
            'userAgent' => self::DEFAULT_USER_AGENT,
            'headers' => [
                'X-API-Key' => $dataMechanicsToken,
            ],
        ]);
    }

    private function sendRequest(Request $request): array
    {
        try {
            $response = $this->client->send($request);
            $data = json_decode($response->getBody()->getContents(), true, self::JSON_DEPTH, JSON_THROW_ON_ERROR);
            return $data ?: [];
        } catch (GuzzleException $e) {
            throw new UserException($e->getMessage(), $e->getCode(), $e);
        }
    }

    public function createApp(array $jobData): array
    {
        $request = new Request('POST', 'apps', [], \GuzzleHttp\json_encode($jobData));
        return $this->sendRequest($request);
    }

    protected function initClient(array $options = []): GuzzleClient
    {
        // Initialize handlers (start with those supplied in constructor)
        if (isset($options['handler']) && $options['handler'] instanceof HandlerStack) {
            $handlerStack = HandlerStack::create($options['handler']);
        } else {
            $handlerStack = HandlerStack::create();
        }
        // Set exponential backoff
        $handlerStack->push(Middleware::retry($this->createDefaultDecider($options['backoffMaxTries'])));
        // Set handler to set default headers
        $handlerStack->push(Middleware::mapRequest(
            function (RequestInterface $request) use ($options) {
                $requestUpdated = $request
                    ->withHeader('User-Agent', $options['userAgent'])
                    ->withHeader('Content-type', 'application/json');
                foreach ($options['headers'] as $key => $value) {
                    $requestUpdated = $requestUpdated->withHeader($key, $value);
                }
                return $requestUpdated;
            }
        ));
        // Set client logger
        if (isset($options['logger']) && $options['logger'] instanceof LoggerInterface) {
            $handlerStack->push(Middleware::log(
                $options['logger'],
                new MessageFormatter('[datamechanics-api] {method} {uri} : {code} {res_header_Content-Length}')
            ));
        }
        // finally create the instance
        return new GuzzleClient(['base_uri' => $options['apiUrl'], 'handler' => $handlerStack]);
    }

    protected function createDefaultDecider(int $maxRetries): Closure
    {
        return function (
            $retries,
            RequestInterface $request,
            ?ResponseInterface $response = null,
            $error = null
        ) use ($maxRetries) {
            if ($retries >= $maxRetries) {
                return false;
            } elseif ($response && $response->getStatusCode() >= 500) {
                return true;
            } elseif ($error) {
                return true;
            } else {
                return false;
            }
        };
    }
}

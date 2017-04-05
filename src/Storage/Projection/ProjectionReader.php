<?php

namespace Honeybee\Elasticsearch2\Storage\Projection;

use Assert\Assertion;
use Elasticsearch\Common\Exceptions\Missing404Exception;
use Honeybee\Elasticsearch2\Storage\ElasticsearchStorage;
use Honeybee\Infrastructure\Config\ConfigInterface;
use Honeybee\Infrastructure\Config\Settings;
use Honeybee\Infrastructure\Config\SettingsInterface;
use Honeybee\Infrastructure\DataAccess\Connector\ConnectorInterface;
use Honeybee\Infrastructure\DataAccess\Storage\StorageReaderInterface;
use Honeybee\Infrastructure\DataAccess\Storage\StorageReaderIterator;
use Honeybee\Projection\ProjectionTypeMap;
use Psr\Log\LoggerInterface;

class ProjectionReader extends ElasticsearchStorage implements StorageReaderInterface
{
    const READ_ALL_LIMIT = 10;

    protected $offset = 0;

    protected $projectionTypeMap;

    public function __construct(
        ConnectorInterface $connector,
        ConfigInterface $config,
        LoggerInterface $logger,
        ProjectionTypeMap $projectionTypeMap
    ) {
        parent::__construct($connector, $config, $logger);

        $this->projection_type_map = $projectionTypeMap;
    }

    public function readAll(SettingsInterface $settings = null)
    {
        $settings = $settings ?: new Settings;

        $data = [];

        $defaultLimit = $this->config->get('limit', self::READ_ALL_LIMIT);
        $limit = $settings->get('limit', $defaultLimit);

        $queryParams = [
            'index' => $this->getIndex(),
            'type' => $this->getType(),
            'size' => $limit,
            'body' => ['query' => ['match_all' => []]]
        ];

        if (!$settings->get('first', true)) {
            if (!$this->offset) {
                return $data;
            }
            $queryParams['from'] = $this->offset;
        }

        $rawResult = $this->connector->getConnection()->search($queryParams);

        $resultHits = $rawResult['hits'];
        foreach ($resultHits['hits'] as $dataRow) {
            $data[] = $this->createResult($dataRow['_source']);
        }

        if ($resultHits['total'] === $this->offset + 1) {
            $this->offset = 0;
        } else {
            $this->offset += $limit;
        }

        return $data;
    }

    public function read($identifier, SettingsInterface $settings = null)
    {
        try {
            $result = $this->connector->getConnection()->get(
                [
                    'index' => $this->getIndex(),
                    'type' => $this->getType(),
                    'id' => $identifier
                ]
            );
        } catch (Missing404Exception $missingError) {
            return null;
        }

        return $this->createResult($result['_source']);
    }

    public function getIterator()
    {
        return new StorageReaderIterator($this);
    }

    private function createResult(array $resultData)
    {
        Assertion::notEmptyKey($resultData, self::OBJECT_TYPE);

        return $this->projection_type_map
            ->getItem($resultData[self::OBJECT_TYPE])
            ->createEntity($resultData);
    }
}

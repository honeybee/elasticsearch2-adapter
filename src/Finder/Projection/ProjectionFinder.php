<?php

namespace Honeybee\Elasticsearch2\Finder\Projection;

use Honeybee\Common\Error\RuntimeError;
use Honeybee\Infrastructure\Config\ConfigInterface;
use Honeybee\Infrastructure\DataAccess\Connector\ConnectorInterface;
use Honeybee\Elasticsearch2\Finder\ElasticsearchFinder;
use Honeybee\Projection\ProjectionTypeMap;
use Psr\Log\LoggerInterface;

class ProjectionFinder extends ElasticsearchFinder
{
    public function __construct(
        ConnectorInterface $connector,
        ConfigInterface $config,
        LoggerInterface $logger,
        ProjectionTypeMap $projectionTypeMap
    ) {
        parent::__construct($connector, $config, $logger);

        $this->projection_type_map = $projectionTypeMap;
    }

    private function createResult(array $documentData)
    {
        $source = $documentData['_source'];
        $eventType = isset($source[self::OBJECT_TYPE]) ? $source[self::OBJECT_TYPE] : false;
        if (!$eventType) {
            throw new RuntimeError(
                'Invalid or corrupt type information within projection data for _id: ' . @$documentData['_id'] ?: ''
            );
        }
        unset($source[self::OBJECT_TYPE]);

        return $this->projection_type_map->getItem($eventType)->createEntity($source);
    }

    protected function mapResultData(array $resultData)
    {
        if ($this->config->get('log_result_data', false) === true) {
            $this->logger->debug('['.__METHOD__.'] raw result = ' . json_encode($resultData, JSON_PRETTY_PRINT));
        }

        $results = [];
        if (isset($resultData['_source'])) {
            // Handling for single document
            $results[] = $this->createResult($resultData);
        } elseif (isset($resultData['hits'])) {
            // Handling for search results
            $hits = $resultData['hits'];
            foreach ($hits['hits'] as $hit) {
                $results[] = $this->createResult($hit);
            }
        } elseif (isset($resultData['docs'])) {
            // Handling for multi-get documents
            $docs = $resultData['docs'];
            foreach ($docs as $doc) {
                if (true === $doc['found']) {
                    $results[] = $this->createResult($doc);
                }
            }
        } else {
            throw new RuntimeError(sprintf('Unsupported result data format: %s', var_export($resultData, true)));
        }

        return $results;
    }
}

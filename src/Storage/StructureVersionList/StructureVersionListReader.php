<?php

namespace Honeybee\Elasticsearch2\Storage\StructureVersionList;

use Elasticsearch\Common\Exceptions\Missing404Exception;
use Honeybee\Elasticsearch2\Storage\ElasticsearchStorage;
use Honeybee\Infrastructure\Config\Settings;
use Honeybee\Infrastructure\Config\SettingsInterface;
use Honeybee\Infrastructure\DataAccess\Storage\StorageReaderInterface;
use Honeybee\Infrastructure\DataAccess\Storage\StorageReaderIterator;
use Honeybee\Infrastructure\Migration\StructureVersion;
use Honeybee\Infrastructure\Migration\StructureVersionList;

class StructureVersionListReader extends ElasticsearchStorage implements StorageReaderInterface
{
    const READ_ALL_LIMIT = 10;

    protected $offset = 0;

    public function readAll(SettingsInterface $settings = null)
    {
        $settings = $settings ?: new Settings;

        $data = [];

        $defaultLimit = $this->config->get('limit', self::READ_ALL_LIMIT);
        $queryParams = [
            'index' => $this->getIndex(),
            'type' => $this->getType(),
            'size' => $settings->get('limit', $defaultLimit),
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
            $data[] = $this->createStructureVersionList($dataRow['_source']);
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
        } catch (Missing404Exception $error) {
            return null;
        }

        return $this->createStructureVersionList($result['_source']);
    }

    public function getIterator()
    {
        return new StorageReaderIterator($this);
    }

    protected function createStructureVersionList(array $data)
    {
        $structureVersionList = new StructureVersionList($data['identifier']);

        foreach ($data['versions'] as $versionData) {
            $structureVersionList->push(new StructureVersion($versionData));
        }

        return $structureVersionList;
    }
}

<?php

namespace Honeybee\Elasticsearch2\Migration;

use Elasticsearch\Common\Exceptions\BadRequest400Exception;
use Elasticsearch\Common\Exceptions\Missing404Exception;
use Honeybee\Common\Error\RuntimeError;
use Honeybee\Common\Util\JsonToolkit;
use Honeybee\Infrastructure\Migration\Migration;
use Honeybee\Infrastructure\Migration\MigrationTargetInterface;

abstract class ElasticsearchMigration extends Migration
{
    const SCROLL_SIZE = 1000;

    const SCROLL_TIMEOUT = '30s';

    abstract protected function getIndexSettingsPath(MigrationTargetInterface $migrationTarget);

    abstract protected function getTypeMappingPaths(MigrationTargetInterface $migrationTarget);

    protected function createIndexIfNotExists(
        MigrationTargetInterface $migrationTarget,
        $registerTypeMapping = false
    ) {
        $indexApi = $this->getConnection($migrationTarget)->indices();
        $params = ['index' => $this->getIndexName($migrationTarget)];
        if (!$indexApi->exists($params) && !$this->getAliasMapping($migrationTarget)) {
            $this->createIndex($migrationTarget, $registerTypeMapping);
        } else {
            $this->updateMappings($migrationTarget);
        }
    }

    protected function createIndex(MigrationTargetInterface $migrationTarget, $registerTypeMapping = false)
    {
        $indexApi = $this->getConnection($migrationTarget)->indices();
        $indexApi->create(
            $this->getIndexSettings($migrationTarget, $registerTypeMapping)
        );
    }

    protected function deleteIndex(MigrationTargetInterface $migrationTarget)
    {
        $indexApi = $this->getConnection($migrationTarget)->indices();
        $params = ['index' => $this->getIndexName($migrationTarget)];
        if ($indexApi->exists($params)) {
            $indexApi->delete($params);
        }
    }

    protected function updateMappings(MigrationTargetInterface $migrationTarget, $reindexIfRequired = false)
    {
        $indexApi = $this->getConnection($migrationTarget)->indices();
        $indexName = $this->getIndexName($migrationTarget);
        $reindexRequired = false;

        foreach ($this->getTypeMappings($migrationTarget) as $typeName => $mapping) {
            try {
                $indexApi->putMapping(
                    [
                        'index' => $indexName,
                        'type' => $typeName,
                        'body' => [$typeName => $mapping]
                    ]
                );
            } catch (BadRequest400Exception $error) {
                if (!$reindexIfRequired) {
                    throw $error;
                }
                $reindexRequired = true;
            }
        }

        if (true === $reindexRequired && true === $reindexIfRequired) {
            $this->updateMappingsWithReindex($migrationTarget);
        }
    }

    protected function updateMappingsWithReindex(MigrationTargetInterface $migrationTarget)
    {
        $client = $this->getConnection($migrationTarget);
        $indexApi = $client->indices();
        $indexName = $this->getIndexName($migrationTarget);
        $aliases = $this->getAliasMapping($migrationTarget);

        if (count($aliases) > 1) {
            throw new RuntimeError(sprintf(
                'Aborting reindexing because there is more than one index mapped to the alias: %s',
                $indexName
            ));
        }

        // Allow index settings override
        $indexSettings = $this->getIndexSettings($migrationTarget);
        $indexSettings = isset($indexSettings['body'])
            ? $indexSettings['body']
            : current($indexApi->getSettings(['index' => $indexName]));

        // Load existing mappings from previous index
        $indexMappings = current($indexApi->getMapping(['index' => $indexName]));
        $currentIndex = key($aliases);
        $newIndex = sprintf('%s_%s', $indexName, $this->getTimestamp());

        // Merge mappings from new index settings if provided
        if (isset($indexSettings['mappings'])) {
            foreach ($indexSettings['mappings'] as $typeName => $mapping) {
                $indexMappings['mappings'] = array_replace(
                    $indexMappings['mappings'],
                    [$typeName => $mapping]
                );
            }
            unset($indexSettings['mappings']);
        }

        // Replace existing mappings with new ones
        foreach ($this->getTypeMappings($migrationTarget) as $typeName => $mapping) {
            $indexMappings['mappings'] = array_replace(
                $indexMappings['mappings'],
                [$typeName => $mapping]
            );
        }

        // Create the new index
        $indexApi->create(
            [
                'index' => $newIndex,
                'body' => array_merge($indexSettings, $indexMappings)
            ]
        );

        // Copy documents from current index to new index
        $response = $client->search(
            [
                'search_type' => 'scan',
                'scroll' => self::SCROLL_TIMEOUT,
                'size' => self::SCROLL_SIZE,
                'index'=> $currentIndex
            ]
        );
        $scrollId = $response['_scroll_id'];
        $totalDocs = $response['hits']['total'];

        while (true) {
            $response = $client->scroll(['scroll_id' => $scrollId, 'scroll' => self::SCROLL_TIMEOUT]);
            if (count($response['hits']['hits']) > 0) {
                foreach ($response['hits']['hits'] as $document) {
                    $bulk[]['index'] = [
                        '_index' => $newIndex,
                        '_type' => $document['_type'],
                        '_id' => $document['_id']
                    ];
                    $bulk[] = $document['_source'];
                }
                $client->bulk(['body' => $bulk]);
                unset($bulk);
                $scrollId = $response['_scroll_id'];
            } else {
                break;
            }
        }

        // Check reindexed document count is correct
        $indexApi->flush();
        $newCount = $client->count(['index' => $newIndex])['count'];
        if ($totalDocs != $newCount) {
            throw new RuntimeError(sprintf(
                'Aborting migration because document count of %s after reindexing does not match expected count of %s',
                $newCount,
                $totalDocs
            ));
        }

        // Switch aliases from old to new index
        $actions = [
            ['remove' => ['alias' => $indexName, 'index' => $currentIndex]],
            ['add' => ['alias' => $indexName, 'index' => $newIndex]]
        ];
        $indexApi->updateAliases(['body' => ['actions' => $actions]]);
    }

    protected function updateIndexTemplates(MigrationTargetInterface $migrationTarget, array $templates)
    {
        $indexApi = $this->getConnection($migrationTarget)->indices();
        foreach ($templates as $templateName => $templateFile) {
            if (!is_readable($templateFile)) {
                throw new RuntimeError(sprintf('Unable to read index template at: %s', $templateFile));
            }
            $template = JsonToolkit::parse(file_get_contents($templateFile));
            $indexApi->putTemplate(['name' => $templateName, 'body' => $template]);
        }
    }

    protected function createSearchTemplates(MigrationTargetInterface $migrationTarget, array $templates)
    {
        $client = $this->getConnection($migrationTarget);
        foreach ($templates as $templateName => $templateFile) {
            if (!is_readable($templateFile)) {
                throw new RuntimeError(sprintf('Unable to read search template at: %s', $templateFile));
            }
            $client->putTemplate(
                [
                    'id' => $templateName,
                    'body' => file_get_contents($templateFile)
                ]
            );
        }
    }

    protected function getIndexSettings(MigrationTargetInterface $migrationTarget, $includeTypeMapping = false)
    {
        $settingsJsonFile = $this->getIndexSettingsPath($migrationTarget);

        if (empty($settingsJsonFile)) {
            return [];
        }

        if (!is_readable($settingsJsonFile)) {
            throw new RuntimeError(sprintf('Unable to read settings for index at: %s', $settingsJsonFile));
        }

        // Index is created with migration timestamp suffix and aliased in order to support
        // zero down-time migrations
        $indexName = $this->getIndexName($migrationTarget);
        $indexSettings['index'] = sprintf('%s_%s', $indexName, $this->getTimestamp());
        $indexSettings['body'] = JsonToolkit::parse(file_get_contents($settingsJsonFile));
        $indexSettings['body']['aliases'][$indexName] = new \stdClass();

        if ($includeTypeMapping) {
            $typeMappings = $this->getTypeMappings($migrationTarget);
            if (isset($indexSettings['body']['mappings'])) {
                $indexSettings['body']['mappings'] = array_merge(
                    $indexSettings['body']['mappings'],
                    $typeMappings
                );
            } else {
                $indexSettings['body']['mappings'] = $typeMappings;
            }
        }

        return $indexSettings;
    }

    protected function getAliasMapping(MigrationTargetInterface $migrationTarget)
    {
        $aliases = [];
        $indexApi = $this->getConnection($migrationTarget)->indices();

        try {
            $aliases = $indexApi->getAlias(['name' => $this->getIndexName($migrationTarget)]);
        } catch (Missing404Exception $error) {
        }

        return $aliases;
    }

    protected function getIndexName(MigrationTargetInterface $migrationTarget)
    {
        return $migrationTarget->getConfig()->get('index');
    }

    protected function getTypeMappings(MigrationTargetInterface $migrationTarget)
    {
        $mappings = [];
        $paths = (array) $this->getTypeMappingPaths($migrationTarget);

        foreach ($paths as $typeName => $mappingFile) {
            if (!is_readable($mappingFile)) {
                throw new RuntimeError(sprintf('Unable to read type-mapping at: %s', $mappingFile));
            }
            $mappings[$typeName] = JsonToolkit::parse(file_get_contents($mappingFile));
        }

        return $mappings;
    }
}

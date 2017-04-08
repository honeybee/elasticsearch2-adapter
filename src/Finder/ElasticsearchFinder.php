<?php

namespace Honeybee\Elasticsearch2\Finder;

use Assert\Assertion;
use Elasticsearch\Common\Exceptions\Missing404Exception;
use Honeybee\Common\Error\RuntimeError;
use Honeybee\Infrastructure\Config\Settings;
use Honeybee\Infrastructure\DataAccess\Finder\Finder;
use Honeybee\Infrastructure\DataAccess\Finder\FinderResult;

abstract class ElasticsearchFinder extends Finder
{
    abstract protected function mapResultData(array $resultData);

    public function getByIdentifier($identifier)
    {
        Assertion::string($identifier);
        Assertion::notBlank($identifier);

        $index = $this->getIndex();
        $type = $this->getType();

        $this->validateForSingleIndexApi($index, $type);

        $query = array_merge_recursive(
            $this->getParameters('get'),
            [
                'index' => $index,
                'type' => $type,
                'id' => $identifier
            ]
        );

        if ($this->config->get('log_get_query', false) === true) {
            $this->logger->debug('['.__METHOD__.'] get query = ' . json_encode($query, JSON_PRETTY_PRINT));
        }

        try {
            $rawResult = $this->connector->getConnection()->get($query);
            $mappedResults = $this->mapResultData($rawResult);
        } catch (Missing404Exception $error) {
            $mappedResults = [];
        }

        return new FinderResult($mappedResults, count($mappedResults));
    }

    public function getByIdentifiers(array $identifiers)
    {
        Assertion::notEmpty($identifiers);

        $index = $this->getIndex();
        $type = $this->getType();

        $this->validateForSingleIndexApi($index, $type);

        $query = array_merge_recursive(
            $this->getParameters('mget'),
            [
                'index' => $index,
                'type' => $type,
                'body' => [
                    'ids' => $identifiers,
                    // result size bound by number of identifiers provided, not by query
                    'size' => 100000
                ]
            ]
        );

        if ($this->config->get('log_mget_query', false) === true) {
            $this->logger->debug('['.__METHOD__.'] mget query = ' . json_encode($query, JSON_PRETTY_PRINT));
        }

        $rawResult = $this->connector->getConnection()->mget($query);
        $mappedResults = $this->mapResultData($rawResult);

        return new FinderResult($mappedResults, count($mappedResults));
    }

    public function find($query)
    {
        Assertion::isArray($query);

        $query = array_merge_recursive(
            $this->getParameters('search'),
            $query,
            [
                'index' => $this->getIndex(),
                'type' => $this->getType()
            ]
        );

        if ($this->config->get('log_search_query', false) === true) {
            $this->logger->debug('['.__METHOD__.'] search query = ' . json_encode($query, JSON_PRETTY_PRINT));
        }

        $rawResult = $this->connector->getConnection()->search($query);
        $mappedResults = $this->mapResultData($rawResult);

        return new FinderResult(
            $mappedResults,
            $rawResult['hits']['total'],
            isset($query['from']) ? $query['from'] : 0
        );
    }

    public function findByStored($query)
    {
        Assertion::isArray($query);

        $query = array_merge_recursive(
            $this->getParameters('search'),
            $query,
            [
                'index' => $this->getIndex(),
                'type' => $this->getType()
            ]
        );

        if ($this->config->get('log_search_query', false) === true) {
            $this->logger->debug('['.__METHOD__.'] stored query = ' . json_encode($query, JSON_PRETTY_PRINT));
        }

        $rawResult = $this->connector->getConnection()->searchTemplate($query);
        $mappedResults = $this->mapResultData($rawResult);

        return new FinderResult(
            $mappedResults,
            $rawResult['hits']['total'],
            isset($query['body']['params']['from']) ? $query['body']['params']['from'] : 0
        );
    }

    public function scrollStart($query, $cursor = null)
    {
        Assertion::isArray($query);

        $query = array_merge_recursive(
            $this->getParameters('search'), //scroll is equivalent to search
            $query,
            [
                'index' => $this->getIndex(),
                'type' => $this->getType(),
                'scroll' => $this->config->get('scroll_timeout', '1m'),
                'sort' => ['_doc']
            ]
        );

        if ($this->config->get('log_search_query', false) === true) {
            $this->logger->debug('['.__METHOD__.'] scroll start = ' . json_encode($query, JSON_PRETTY_PRINT));
        }

        // Elasticsearch returns results and scroll id on scroll start
        $rawResult = $this->connector->getConnection()->search($query);
        $mappedResults = $this->mapResultData($rawResult);

        return new FinderResult(
            $mappedResults,
            $rawResult['hits']['total'],
            0, // no known offset during scroll
            $rawResult['_scroll_id']
        );
    }

    public function scrollNext($cursor, $size = null)
    {
        Assertion::string($cursor);
        Assertion::notBlank($cursor);

        $query = [
            'scroll_id' => $cursor,
            'scroll' => $this->config->get('scroll_timeout', '1m')
        ];

        if ($this->config->get('log_search_query', false) === true) {
            $this->logger->debug('['.__METHOD__.'] scroll next = ' . json_encode($query, JSON_PRETTY_PRINT));
        }

        $rawResult = $this->connector->getConnection()->scroll($query);
        $mappedResults = $this->mapResultData($rawResult);

        return new FinderResult(
            $mappedResults,
            $rawResult['hits']['total'],
            0, // unknown offset during scroll
            $rawResult['_scroll_id']
        );
    }

    public function scrollEnd($cursor)
    {
        Assertion::string($cursor);
        Assertion::notBlank($cursor);

        if ($this->config->get('log_search_query', false) === true) {
            $this->logger->debug('['.__METHOD__.'] scroll end ' . $cursor);
        }

        $this->connector->getConnection()->clearScroll(['scroll_id' => $cursor]);
    }

    /**
     * @return array
     */
    protected function getIndex()
    {
        $fallBackIndex = $this->connector->getConfig()->get('index', '_all');
        $index = $this->config->get('index', $fallBackIndex);
        Assertion::notEmpty($index);
        if (is_scalar($index)) {
            $index = explode(',', $index);
        }
        return (array)$index;
    }

    /**
     * @return array
     */
    protected function getType()
    {
        $fallBackType = $this->connector->getConfig()->get('type', '_all');
        $type = $this->config->get('type', $fallBackType);
        Assertion::notEmpty($type);
        if (is_scalar($type)) {
            $type = explode(',', $type);
        }
        return (array)$type;
    }

    /**
     * @return array
     */
    protected function getParameters($method)
    {
        return (array)$this->config->get('parameters', new Settings)->get($method);
    }

    private function validateForSingleIndexApi(array $index, array $type)
    {
        if (count($index) > 1) {
            throw new RuntimeError(
                sprintf(
                    'Elasticsearch single index API does not support multiple indices, "%s" given.',
                    implode(',', $index)
                )
            );
        }

        if (count($type) > 1 && (count($index) > 1 || current($index) !== '_all')) {
            throw new RuntimeError(
                sprintf(
                    'Elasticsearch multiple type single index API only supports index "_all", "%s" given.',
                    implode(',', $index)
                )
            );
        }
    }
}

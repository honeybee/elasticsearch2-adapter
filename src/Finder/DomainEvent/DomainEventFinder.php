<?php

namespace Honeybee\Elasticsearch2\Finder\DomainEvent;

use Honeybee\Common\Error\RuntimeError;
use Honeybee\Elasticsearch2\Finder\ElasticsearchFinder;
use Honeybee\Model\Event\AggregateRootEventInterface;

class DomainEventFinder extends ElasticsearchFinder
{
    protected function mapResultData(array $resultData)
    {
        if (!isset($resultData['hits']) || !isset($resultData['hits']['hits'])) {
            throw new RuntimeError('Expected "hits" in result data but not found.');
        }

        $results = [];
        foreach ($resultData['hits']['hits'] as $hit) {
            $eventData = $hit['_source'];
            $eventType = isset($eventData[self::OBJECT_TYPE]) ? $eventData[self::OBJECT_TYPE] : false;
            if (!$eventType || !class_exists($eventType, true)) {
                throw new RuntimeError('Invalid or corrupt type information within event data.');
            }
            unset($eventData[self::OBJECT_TYPE]);

            $domainEvent = new $eventType($eventData);
            if (!$domainEvent instanceof AggregateRootEventInterface) {
                throw new RuntimeError(
                    sprintf(
                        'Non-event object given within result data. %s only supports instances of %s.',
                        __CLASS__,
                        AggregateRootEventInterface::CLASS
                    )
                );
            }
            $results[] = $domainEvent;
        }

        return $results;
    }
}

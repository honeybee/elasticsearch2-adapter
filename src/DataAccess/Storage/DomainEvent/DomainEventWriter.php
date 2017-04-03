<?php

namespace Honeybee\Elasticsearch2\DataAccess\Storage\DomainEvent;

use Honeybee\Common\Error\RuntimeError;
use Honeybee\Elasticsearch2\DataAccess\Storage\ElasticsearchStorageWriter;
use Honeybee\Model\Event\AggregateRootEventInterface;
use Honeybee\Infrastructure\Config\SettingsInterface;

class DomainEventWriter extends ElasticsearchStorageWriter
{
    public function write($domain_event, SettingsInterface $settings = null)
    {
        if (!$domain_event instanceof AggregateRootEventInterface) {
            throw new RuntimeError(
                sprintf(
                    'Invalid payload given to %s, expected type of %s',
                    __METHOD__,
                    AggregateRootEventInterface::CLASS
                )
            );
        }

        $this->writeData($domain_event->getUuid(), $domain_event->toArray(), $settings);
    }
}

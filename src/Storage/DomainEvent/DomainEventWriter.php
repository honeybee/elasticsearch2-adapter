<?php

namespace Honeybee\Elasticsearch2\Storage\DomainEvent;

use Honeybee\Common\Error\RuntimeError;
use Honeybee\Elasticsearch2\DataAccess\Storage\ElasticsearchStorageWriter;
use Honeybee\Model\Event\AggregateRootEventInterface;
use Honeybee\Infrastructure\Config\SettingsInterface;

class DomainEventWriter extends ElasticsearchStorageWriter
{
    public function write($domainEvent, SettingsInterface $settings = null)
    {
        if (!$domainEvent instanceof AggregateRootEventInterface) {
            throw new RuntimeError(
                sprintf(
                    'Invalid payload given to %s, expected type of %s',
                    __METHOD__,
                    AggregateRootEventInterface::CLASS
                )
            );
        }

        $this->writeData($domainEvent->getUuid(), $domainEvent->toArray(), $settings);
    }
}

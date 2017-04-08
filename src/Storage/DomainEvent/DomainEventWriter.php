<?php

namespace Honeybee\Elasticsearch2\Storage\DomainEvent;

use Assert\Assertion;
use Honeybee\Elasticsearch2\Storage\ElasticsearchStorageWriter;
use Honeybee\Infrastructure\Config\SettingsInterface;
use Honeybee\Model\Event\AggregateRootEventInterface;

class DomainEventWriter extends ElasticsearchStorageWriter
{
    public function write($domainEvent, SettingsInterface $settings = null)
    {
        Assertion::isInstanceOf($domainEvent, AggregateRootEventInterface::CLASS);

        $this->writeData($domainEvent->getUuid(), $domainEvent->toArray(), $settings);
    }
}

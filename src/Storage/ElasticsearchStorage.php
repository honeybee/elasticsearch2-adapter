<?php

namespace Honeybee\Elasticsearch2\Storage;

use Honeybee\Infrastructure\Config\Settings;
use Honeybee\Infrastructure\DataAccess\Storage\Storage;

abstract class ElasticsearchStorage extends Storage
{
    protected function getIndex()
    {
        $fallbackIndex = $this->connector->getConfig()->get('index');

        return $this->config->get('index', $fallbackIndex);
    }

    protected function getType()
    {
        $fallbackType = $this->connector->getConfig()->get('type');

        return $this->config->get('type', $fallbackType);
    }

    protected function getParameters($method)
    {
        return (array)$this->config->get('parameters', new Settings)->get($method);
    }
}

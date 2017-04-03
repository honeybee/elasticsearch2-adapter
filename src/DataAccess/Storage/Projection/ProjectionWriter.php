<?php

namespace Honeybee\Elasticsearch2\DataAccess\Storage\Projection;

use Honeybee\Common\Error\RuntimeError;
use Honeybee\Elasticsearch2\DataAccess\Storage\ElasticsearchStorageWriter;
use Honeybee\Infrastructure\Config\Settings;
use Honeybee\Infrastructure\Config\SettingsInterface;
use Honeybee\Projection\ProjectionInterface;
use Honeybee\Projection\ProjectionMap;

class ProjectionWriter extends ElasticsearchStorageWriter
{
    public function write($projection, SettingsInterface $settings = null)
    {
        if (!$projection instanceof ProjectionInterface) {
            throw new RuntimeError(
                sprintf('Invalid payload given to %s, expected type of %s', __METHOD__, ProjectionInterface::CLASS)
            );
        }

        $this->writeData($projection->getIdentifier(), $projection->toArray(), $settings);
    }

    public function writeMany($projections, SettingsInterface $settings = null)
    {
        if (!$projections instanceof ProjectionMap) {
            throw new RuntimeError(
                sprintf('Invalid payload given to %s, expected type of %s', __METHOD__, ProjectionMap::CLASS)
            );
        }

        if ($projections->isEmpty()) {
            return;
        } elseif ($projections->getSize() === 1) {
            return $this->write($projections->toList()->getFirst(), $settings);
        }

        $this->writeBulk($projections->toArray(), $settings);
    }
}

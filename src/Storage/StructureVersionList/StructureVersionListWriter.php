<?php

namespace Honeybee\Elasticsearch2\Storage\StructureVersionList;

use Assert\Assertion;
use Honeybee\Elasticsearch2\Storage\ElasticsearchStorageWriter;
use Honeybee\Infrastructure\Config\SettingsInterface;
use Honeybee\Infrastructure\Migration\StructureVersionList;

class StructureVersionListWriter extends ElasticsearchStorageWriter
{
    public function write($structureVersionList, SettingsInterface $settings = null)
    {
        Assertion::isInstanceOf($structureVersionList, StructureVersionList::CLASS);

        $this->writeData(
            $structureVersionList->getIdentifier(),
            [
                'identifier' => $structureVersionList->getIdentifier(),
                'versions' => $structureVersionList->toArray()
            ]
        );
    }
}

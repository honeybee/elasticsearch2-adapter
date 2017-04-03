<?php

namespace Honeybee\Elasticsearch2\DataAccess\Storage\StructureVersionList;

use Assert\Assertion;
use Honeybee\Elasticsearch2\DataAccess\Storage\ElasticsearchStorageWriter;
use Honeybee\Infrastructure\Config\SettingsInterface;
use Honeybee\Infrastructure\Migration\StructureVersionList;

class StructureVersionListWriter extends ElasticsearchStorageWriter
{
    public function write($structure_version_list, SettingsInterface $settings = null)
    {
        Assertion::isInstanceOf($structure_version_list, StructureVersionList::CLASS);

        $this->writeData(
            $structure_version_list->getIdentifier(),
            [
                'identifier' => $structure_version_list->getIdentifier(),
                'versions' => $structure_version_list->toArray()
            ]
        );
    }
}

<?php

namespace Honeybee\Tests\Elasticsearch2\Finder;

use Honeybee\Elasticsearch2\Finder\CustomQueryTranslation;
use Honeybee\Infrastructure\DataAccess\Query\CustomQueryInterface;
use Honeybee\Infrastructure\DataAccess\Query\QueryInterface;
use Honeybee\Tests\Elasticsearch2\TestCase;
use Mockery;

class CustomQueryTranslationTest extends TestCase
{
    /**
     * @expectedException Assert\InvalidArgumentException
     */
    public function testTranslateUnsupportedQuery()
    {
        $query = Mockery::mock(QueryInterface::CLASS);
        (new CustomQueryTranslation)->translate($query);
    }

    public function testTranslateQuery()
    {
        $queryData = [
            'body' => [
                'id' => 'test',
                'params' => ['from' => 0, 'size' => 10]
            ]

        ];

        $query = Mockery::mock(CustomQueryInterface::CLASS);
        $query->shouldReceive('getQuery')->once()->withNoArgs()->andReturn($queryData);
        $translation = (new CustomQueryTranslation)->translate($query);

        $this->assertEquals($queryData, $translation);
    }
}

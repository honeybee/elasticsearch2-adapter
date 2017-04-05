<?php

namespace Honeybee\Tests\Elasticsearch2\Finder;

use Honeybee\Elasticsearch2\Finder\CriteriaQueryTranslation;
use Honeybee\Infrastructure\Config\ArrayConfig;
use Honeybee\Infrastructure\DataAccess\Query\StoredQuery;
use Honeybee\Infrastructure\DataAccess\Query\QueryInterface;
use Honeybee\Tests\Elasticsearch2\TestCase;

class CriteriaQueryTranslationTest extends TestCase
{
    /**
     * @dataProvider provideQueryFixture
     */
    public function testTranslate(QueryInterface $query, array $expectedEsQuery)
    {
        $esQuery = (
            new CriteriaQueryTranslation($this->getQueryTranslationConfig())
        )->translate($query);

        $this->assertEquals($expectedEsQuery, $esQuery);
    }

    /**
     * @expectedException Assert\InvalidArgumentException
     */
    public function testTranslateUnsupportedQuery()
    {
        $query = new StoredQuery('invalid', [], 0, 1);
        (new CriteriaQueryTranslation($this->getQueryTranslationConfig()))->translate($query);
    }

    /**
     * @codeCoverageIgnore
     */
    public function provideQueryFixture()
    {
        return include __DIR__ . '/Fixture/criteria_query_translations.php';
    }

    private function getQueryTranslationConfig()
    {
        return new ArrayConfig([
            'multi_fields' => ['username']
        ]);
    }
}

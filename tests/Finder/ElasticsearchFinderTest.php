<?php

namespace Honeybee\Tests\Elasticsearch2\Finder;

use Elasticsearch\Client;
use Elasticsearch\Common\Exceptions\Missing404Exception;
use Honeybee\Elasticsearch2\Connector\ElasticsearchConnector;
use Honeybee\Elasticsearch2\Finder\ElasticsearchFinder;
use Honeybee\Infrastructure\Config\ArrayConfig;
use Honeybee\Infrastructure\DataAccess\Finder\FinderResult;
use Honeybee\Tests\Elasticsearch2\TestCase;
use Mockery;
use Psr\Log\NullLogger;

class ElasticsearchFinderTest extends TestCase
{
    private $mockClient;

    private $mockConnector;

    private function makeFinder(array $config = [])
    {
        return Mockery::mock(
            ElasticsearchFinder::CLASS.'[mapResultData]',
            [
                $this->mockConnector,
                new ArrayConfig($config),
                new NullLogger
            ]
        )->shouldAllowMockingProtectedMethods('mapResultData');
    }

    public function setUp()
    {
        $this->mockClient = Mockery::mock(Client::CLASS);
        $this->mockConnector = Mockery::mock(ElasticsearchConnector::CLASS);
        $this->mockConnector->shouldReceive('isConnected')->never();
    }

    /**
     * @expectedException Assert\InvalidArgumentException
     */
    public function testGetByIdentifierNumeric()
    {
        $this->makeFinder()->getByIdentifier(123);
    }

    /**
     * @expectedException Assert\InvalidArgumentException
     */
    public function testGetByIdentifierEmpty()
    {
        $this->makeFinder()->getByIdentifier('');
    }

    /**
     * @expectedException Assert\InvalidArgumentException
     */
    public function testGetByIdentifierIndexEmpty()
    {
        $this->mockConnector->shouldReceive('getConfig')->once()->withNoArgs()->andReturn(new ArrayConfig([]));
        $this->makeFinder(['index' => ''])->getByIdentifier('test_id');
    }

    /**
     * @expectedException Honeybee\Common\Error\RuntimeError
     */
    public function testGetByIdentifierIndexArrayMultiple()
    {
        $this->mockConnector->shouldReceive('getConfig')->twice()->withNoArgs()->andReturn(new ArrayConfig([]));
        $this->makeFinder(['index' => ['index1', 'index2'], 'type' => 'type1'])->getByIdentifier('test_id');
    }

    /**
     * @expectedException Honeybee\Common\Error\RuntimeError
     */
    public function testGetByIdentifierIndexStringMultiple()
    {
        $this->mockConnector->shouldReceive('getConfig')->twice()->withNoArgs()->andReturn(new ArrayConfig([]));
        $this->makeFinder(['index' => 'index1,index2', 'type' => 'type1'])->getByIdentifier('test_id');
    }

    /**
     * @expectedException Honeybee\Common\Error\RuntimeError
     */
    public function testGetByIdentifierTypeStringMultiple()
    {
        $this->mockConnector->shouldReceive('getConfig')->twice()->withNoArgs()->andReturn(new ArrayConfig([]));
        $this->makeFinder(['index' => 'index1', 'type' => 'type1,type2'])->getByIdentifier('test_id');
    }

    /**
     * @expectedException Honeybee\Common\Error\RuntimeError
     */
    public function testGetByIdentifierTypeArrayMultiple()
    {
        $this->mockConnector->shouldReceive('getConfig')->twice()->withNoArgs()->andReturn(new ArrayConfig([]));
        $this->makeFinder(['index' => 'index1', 'type' => ['type1', 'type2']])->getByIdentifier('test_id');
    }

    public function testGetByIdentifierMissing404()
    {
        $this->mockClient->shouldReceive('get')->once()->with([
            'index' => ['_all'],
            'type' => ['type1', 'type2'],
            'id' => 'test_id'
        ])->andThrow(Missing404Exception::CLASS);
        $this->mockConnector->shouldReceive('getConfig')->twice()->withNoArgs()->andReturn(new ArrayConfig([]));
        $this->mockConnector->shouldReceive('getConnection')->once()->withNoArgs()->andReturn($this->mockClient);

        $mockFinder = $this->makeFinder(['type' => 'type1,type2']);

        $this->assertEquals(new FinderResult, $mockFinder->getByIdentifier('test_id'));
    }

    public function testGetByIdentifier()
    {
        $testData = ['raw' => 'result'];
        $this->mockClient->shouldReceive('get')->once()->with([
            'index' => ['_all'],
            'type' => ['type1', 'type2'],
            'id' => 'test_id',
            'routing' => 'route'
        ])->andReturn($testData);
        $this->mockConnector->shouldReceive('getConfig')->twice()->withNoArgs()->andReturn(new ArrayConfig([]));
        $this->mockConnector->shouldReceive('getConnection')->once()->withNoArgs()->andReturn($this->mockClient);

        $mockFinder = $this->makeFinder([
            'index' => '_all',
            'type' => 'type1,type2',
            'parameters' => ['get' => ['routing' => 'route']],
            'log_get_query' => true
        ]);
        $resultEntity = new \stdClass($testData);
        $mockFinder->shouldReceive('mapResultData')->once()->with($testData)->andReturn([$resultEntity]);

        $this->assertEquals(new FinderResult([$resultEntity], 1), $mockFinder->getByIdentifier('test_id'));
    }

    /**
     * @expectedException Assert\InvalidArgumentException
     */
    public function testGetByIdentifiersEmpty()
    {
        $this->makeFinder()->getByIdentifiers([]);
    }

    public function testGetByIdentifiers()
    {
        $testData = ['docs' => [['raw' => 'result']]];
        $this->mockClient->shouldReceive('mget')->once()->with([
            'index' => ['_all'],
            'type' => ['_all'],
            'routing' => 'abc',
            'body' => [
                'ids' => ['test_id'],
                'size' => 100000
            ]
        ])->andReturn($testData);
        $this->mockConnector->shouldReceive('getConfig')->twice()->withNoArgs()->andReturn(new ArrayConfig([]));
        $this->mockConnector->shouldReceive('getConnection')->once()->withNoArgs()->andReturn($this->mockClient);

        $mockFinder = $this->makeFinder([
            'log_mget_query' => true,
            'parameters' => ['mget' => ['routing' => 'abc']]
        ]);
        $resultEntity = new \stdClass($testData['docs'][0]);
        $mockFinder->shouldReceive('mapResultData')->once()->with($testData)->andReturn([$resultEntity]);

        $finderResult = $mockFinder->getByIdentifiers(['test_id']);
        $this->assertEquals(new FinderResult([$resultEntity], 1), $finderResult);
    }

    /**
     * @expectedException Assert\InvalidArgumentException
     */
    public function testFindStringQuery()
    {
        $this->makeFinder()->find('invalid query');
    }

    public function testFind()
    {
        $testData = ['hits' => ['total' => 11, 'hits' => [['raw' => 'result']]]];
        $this->mockClient->shouldReceive('search')->once()->with([
            'index' => ['test_index'],
            'type' => ['test_type'],
            'routing' => 'abc',
            'from' => 10,
            'body' => ['match_all' => []]
        ])->andReturn($testData);
        $connectorConfig = new ArrayConfig(['index' => 'test_index', 'type' => ['test_type']]);
        $this->mockConnector->shouldReceive('getConfig')->twice()->withNoArgs()->andReturn($connectorConfig);
        $this->mockConnector->shouldReceive('getConnection')->once()->withNoArgs()->andReturn($this->mockClient);

        $mockFinder = $this->makeFinder([
            'log_search_query' => true,
            'parameters' => ['search' => ['routing' => 'abc']]
        ]);
        $resultEntity = new \stdClass($testData['hits']['hits'][0]);
        $mockFinder->shouldReceive('mapResultData')->once()->with($testData)->andReturn([$resultEntity]);

        $finderResult = $mockFinder->find([
            'from' => 10,
            'body' => ['match_all' => []]
        ]);
        $this->assertEquals(new FinderResult([$resultEntity], 11, 10), $finderResult);
    }

    /**
     * @expectedException Assert\InvalidArgumentException
     */
    public function testFindByStoredStringQuery()
    {
        $this->makeFinder()->findByStored('invalid query');
    }

    public function testFindByStored()
    {
        $testData = ['hits' => ['total' => 11, 'hits' => [['raw' => 'result']]]];
        $this->mockClient->shouldReceive('searchTemplate')->once()->with([
            'index' => ['index1', 'index2'],
            'type' => ['_all'],
            'routing' => 'abc',
            'body' => ['params' => ['key' => 'value', 'from' => 10]]
        ])->andReturn($testData);
        $this->mockConnector->shouldReceive('getConfig')->twice()->withNoArgs()->andReturn(new ArrayConfig([]));
        $this->mockConnector->shouldReceive('getConnection')->once()->withNoArgs()->andReturn($this->mockClient);

        $mockFinder = $this->makeFinder([
            'index' => 'index1,index2',
            'log_search_query' => true,
            'parameters' => ['search' => ['routing' => 'abc']]
        ]);
        $resultEntity = new \stdClass($testData['hits']['hits'][0]);
        $mockFinder->shouldReceive('mapResultData')->once()->with($testData)->andReturn([$resultEntity]);

        $finderResult = $mockFinder->findByStored([
            'body' => ['params' => ['key' => 'value', 'from' => 10]]
        ]);
        $this->assertEquals(new FinderResult([$resultEntity], 11, 10), $finderResult);
    }

    /**
     * @expectedException Assert\InvalidArgumentException
     */
    public function testScrollStartStringQuery()
    {
        $this->makeFinder()->scrollStart('invalid query');
    }

    public function testScrollStart()
    {
        $testData = ['hits' => ['total' => 100, 'hits' => [['raw' => 'result']]], '_scroll_id' => 'scroll_id'];
        $this->mockClient->shouldReceive('search')->once()->with([
            'index' => ['index1', 'index2'],
            'type' => ['type1', 'type2'],
            'routing' => 'abc',
            'scroll' => '2m',
            'sort' => ['_doc'],
            'body' => ['match_all' => []]
        ])->andReturn($testData);
        $this->mockConnector->shouldReceive('getConfig')->twice()->withNoArgs()->andReturn(new ArrayConfig([]));
        $this->mockConnector->shouldReceive('getConnection')->once()->withNoArgs()->andReturn($this->mockClient);

        $mockFinder = $this->makeFinder([
            'index' => 'index1,index2',
            'type' => ['type1', 'type2'],
            'log_search_query' => true,
            'scroll_timeout' => '2m'
        ]);
        $resultEntity = new \stdClass($testData['hits']['hits'][0]);
        $mockFinder->shouldReceive('mapResultData')->once()->with($testData)->andReturn([$resultEntity]);

        $finderResult = $mockFinder->scrollStart([
            'routing' => 'abc',
            'body' => ['match_all' => []]
        ]);
        $this->assertEquals(new FinderResult([$resultEntity], 100, 0, 'scroll_id'), $finderResult);
    }

    /**
     * @expectedException Assert\InvalidArgumentException
     */
    public function testScrollNextInvalidCursor()
    {
        $this->makeFinder()->scrollNext('');
    }

    public function testScrollNext()
    {
        $testData = ['hits' => ['total' => 100, 'hits' => [['raw' => 'result']]], '_scroll_id' => 'next_scroll_id'];
        $this->mockClient->shouldReceive('scroll')->once()->with([
            'scroll_id' => 'test_scroll_id',
            'scroll' => '1m',
        ])->andReturn($testData);
        $this->mockConnector->shouldReceive('getConnection')->once()->withNoArgs()->andReturn($this->mockClient);

        $mockFinder = $this->makeFinder(['log_search_query' => true]);
        $resultEntity = new \stdClass($testData['hits']['hits'][0]);
        $mockFinder->shouldReceive('mapResultData')->once()->with($testData)->andReturn([$resultEntity]);

        $finderResult = $mockFinder->scrollNext('test_scroll_id');
        $this->assertEquals(new FinderResult([$resultEntity], 100, 0, 'next_scroll_id'), $finderResult);
    }

    /**
     * @expectedException Assert\InvalidArgumentException
     */
    public function testScrollEndInvalidCursor()
    {
        $this->makeFinder()->scrollEnd('');
    }

    public function testScrollEnd()
    {
        $this->mockClient->shouldReceive('clearScroll')->once()->with([
            'scroll_id' => 'test_scroll_id'
        ])->andReturnNull();
        $this->mockConnector->shouldReceive('getConnection')->once()->withNoArgs()->andReturn($this->mockClient);

        $mockFinder = $this->makeFinder(['log_search_query' => true]);

        $this->assertNull($mockFinder->scrollEnd('test_scroll_id'));
    }
}

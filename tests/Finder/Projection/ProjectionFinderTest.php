<?php

namespace Honeybee\Tests\Elasticsearch2\Finder\Projection;

use Elasticsearch\Client;
use Honeybee\Elasticsearch2\Connector\ElasticsearchConnector;
use Honeybee\Elasticsearch2\Finder\Projection\ProjectionFinder;
use Honeybee\Infrastructure\Config\ArrayConfig;
use Honeybee\Infrastructure\DataAccess\Finder\FinderResult;
use Honeybee\Projection\ProjectionTypeMap;
use Honeybee\Tests\Elasticsearch2\Fixture\BookSchema\Projection\Book\BookType;
use Honeybee\Tests\Elasticsearch2\TestCase;
use Mockery;
use Psr\Log\NullLogger;

class ProjectionFinderTest extends TestCase
{
    private $mockTypeMap;

    private $mockConnector;

    private $mockClient;

    public function setUp()
    {
        $this->mockTypeMap = Mockery::mock(ProjectionTypeMap::CLASS);
        $this->mockConnector = Mockery::mock(ElasticsearchConnector::CLASS);
        $this->mockClient = Mockery::mock(Client::CLASS);
        $this->mockConnector->shouldReceive('getConfig')->twice()->withNoArgs()->andReturn(new ArrayConfig([]));
        $this->mockConnector->shouldReceive('getConnection')->once()->withNoArgs()->andReturn($this->mockClient);
        $this->mockConnector->shouldReceive('isConnected')->never();
    }

    /**
     * @expectedException Honeybee\Common\Error\RuntimeError
     */
    public function testGetByIdentifierNoSource()
    {
        $this->mockClient->shouldReceive('get')->once()
            ->with(['index' => ['index1'], 'type' => ['type1'], 'id' => 'test_id'])
            ->andReturn([]);


        $projectionFinder = new ProjectionFinder(
            $this->mockConnector,
            new ArrayConfig(['index' => 'index1', 'type' => 'type1']),
            new NullLogger,
            $this->mockTypeMap
        );

        $projectionFinder->getByIdentifier('test_id');
    }

    /**
     * @expectedException Honeybee\Common\Error\RuntimeError
     */
    public function testGetByIdentifierInvalidType()
    {
        $this->mockClient->shouldReceive('get')->once()
            ->with(['index' => ['index1'], 'type' => ['type1'], 'id' => 'test_id'])
            ->andReturn(['_source' => ['missing_type']]);

        $projectionFinder = new ProjectionFinder(
            $this->mockConnector,
            new ArrayConfig(['index' => 'index1', 'type' => 'type1']),
            new NullLogger,
            $this->mockTypeMap
        );

        $projectionFinder->getByIdentifier('test_id');
    }

    public function testGetByIdentifierWithSource()
    {
        $testResult = [
            '_id' => 'test_id',
            '_source' => ['@type' => 'mock_type']
        ];

        $this->mockClient->shouldReceive('get')->once()
            ->with(['index' => ['_all'], 'type' => ['type1'], 'id' => 'test_id'])
            ->andReturn($testResult);
        $projectionType = new BookType;
        $this->mockTypeMap->shouldReceive('getItem')->once()->with('mock_type')->andReturn($projectionType);

        $projectionFinder = new ProjectionFinder(
            $this->mockConnector,
            new ArrayConfig(['type' => 'type1', 'log_result_data' => true]),
            new NullLogger,
            $this->mockTypeMap
        );

        $expectedResult = new FinderResult([$projectionType->createEntity($testResult['_source'])], 1);
        $finderResult = $projectionFinder->getByIdentifier('test_id');

        $this->assertEquals($expectedResult, $finderResult);
    }

    public function testGetByIdentifierWithHits()
    {
        $testResult = [
            'hits' => [
                'hits' => [
                    [
                        '_id' => 'test_id',
                        '_source' => ['@type' => 'mock_type']
                    ]
                ]
            ]
        ];

        $this->mockClient->shouldReceive('get')->once()
            ->with(['index' => ['_all'], 'type' => ['type1', 'type2'], 'id' => 'test_id'])
            ->andReturn($testResult);
        $projectionType = new BookType;
        $this->mockTypeMap->shouldReceive('getItem')->once()->with('mock_type')->andReturn($projectionType);

        $projectionFinder = new ProjectionFinder(
            $this->mockConnector,
            new ArrayConfig(['type' => ['type1', 'type2']]),
            new NullLogger,
            $this->mockTypeMap
        );

        $expectedResult = new FinderResult([
            $projectionType->createEntity($testResult['hits']['hits'][0]['_source'])
        ], 1);
        $finderResult = $projectionFinder->getByIdentifier('test_id');

        $this->assertEquals($expectedResult, $finderResult);
    }

    public function testGetByIdentifierWithDocs()
    {
        $testResult = [
            'docs' => [
                [
                    '_id' => 'test_id',
                    'found' => true,
                    '_source' => [
                        '@type' => 'mock_type',
                    ]
                ]
            ]
        ];

        $this->mockClient->shouldReceive('get')->once()
            ->with(['index' => ['index1'], 'type' => ['type1'], 'id' => 'test_id'])
            ->andReturn($testResult);
        $projectionType = new BookType;
        $this->mockTypeMap->shouldReceive('getItem')->once()->with('mock_type')->andReturn($projectionType);

        $projectionFinder = new ProjectionFinder(
            $this->mockConnector,
            new ArrayConfig(['index' => 'index1', 'type' => 'type1']),
            new NullLogger,
            $this->mockTypeMap
        );

        $expectedResult = new FinderResult([
            $projectionType->createEntity($testResult['docs'][0]['_source'])
        ], 1);
        $finderResult = $projectionFinder->getByIdentifier('test_id');

        $this->assertEquals($expectedResult, $finderResult);
    }
}

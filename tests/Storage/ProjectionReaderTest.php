<?php

namespace Honeybee\Tests\Elasticsearch2\Storage;

use Elasticsearch\Client;
use Elasticsearch\Common\Exceptions\Missing404Exception;
use Honeybee\Elasticsearch2\Storage\Projection\ProjectionReader;
use Honeybee\Infrastructure\Config\ArrayConfig;
use Honeybee\Infrastructure\Config\Settings;
use Honeybee\Infrastructure\DataAccess\Connector\ConnectorInterface;
use Honeybee\Infrastructure\DataAccess\Storage\StorageReaderIterator;
use Honeybee\Projection\ProjectionTypeMap;
use Honeybee\Tests\Elasticsearch2\Fixture\BookSchema\Projection\Book\BookType;
use Honeybee\Tests\Elasticsearch2\Fixture\BookSchema\Projection\Author\AuthorType;
use Honeybee\Tests\Elasticsearch2\TestCase;
use Mockery;
use Psr\Log\NullLogger;

class ProjectionReaderTest extends TestCase
{
    private $projectionTypeMap;

    private $mockConnector;

    private $mockClient;

    public function setUp()
    {
        $bookType = new BookType;
        $authorType = new AuthorType;
        $this->projectionTypeMap = new ProjectionTypeMap([
            $bookType->getVariantPrefix() => $bookType,
            $authorType->getVariantPrefix() => $authorType
        ]);

        $this->mockConnector = Mockery::mock(ConnectorInterface::CLASS);
        $this->mockClient = Mockery::mock(Client::CLASS);
    }

    public function testReadAll()
    {
        $testData = include(__DIR__ . '/Fixture/projection_reader_test_01.php');
        $this->mockConnector->shouldReceive('getConfig')->twice()->andReturn(new ArrayConfig([]));
        $this->mockConnector->shouldReceive('getConnection')->once()->andReturn($this->mockClient);
        $this->mockClient->shouldReceive('search')->once()->with([
            'index' => 'index',
            'type' => 'type',
            'size' => 10,
            'body' => ['query' => ['match_all' => []]]
        ])->andReturn($testData['raw_result']);

        $projectionReader = new ProjectionReader(
            $this->mockConnector,
            new ArrayConfig(['index' => 'index', 'type' => 'type']),
            new NullLogger,
            $this->projectionTypeMap
        );

        $projections = $this->createProjections($testData['raw_result']['hits']['hits']);
        $this->assertEquals($projections, $projectionReader->readAll());
    }

    public function testReadAllNoResults()
    {
        $this->mockConnector->shouldReceive('getConfig')->twice()->andReturn(new ArrayConfig([]));
        $this->mockConnector->shouldReceive('getConnection')->once()->andReturn($this->mockClient);
        $this->mockClient->shouldReceive('search')->once()->with([
            'index' => 'index',
            'type' => 'type',
            'size' => 20,
            'body' => ['query' => ['match_all' => []]]
        ])->andReturn(['hits' => ['total' => 0, 'hits' => []]]);

        $projectionReader = new ProjectionReader(
            $this->mockConnector,
            new ArrayConfig(['index' => 'index', 'type' => 'type']),
            new NullLogger,
            $this->projectionTypeMap
        );

        $this->assertEquals([], $projectionReader->readAll(new Settings(['limit' => 20])));
    }

    public function testReadAllMixedResults()
    {
        $testData = include(__DIR__ . '/Fixture/projection_reader_test_02.php');
        $this->mockConnector->shouldReceive('getConfig')->twice()->andReturn(new ArrayConfig([]));
        $this->mockConnector->shouldReceive('getConnection')->once()->andReturn($this->mockClient);
        $this->mockClient->shouldReceive('search')->once()->with([
            'index' => null,
            'type' => 'type',
            'size' => 5,
            'body' => ['query' => ['match_all' => []]]
        ])->andReturn($testData['raw_result']);

        $projectionReader = new ProjectionReader(
            $this->mockConnector,
            new ArrayConfig(['type' => 'type', 'limit' => 5]),
            new NullLogger,
            $this->projectionTypeMap
        );

        $projections = $this->createProjections($testData['raw_result']['hits']['hits']);
        $this->assertEquals($projections, $projectionReader->readAll());
    }

    public function testRead()
    {
        $testData = include(__DIR__ . '/Fixture/projection_reader_test_03.php');
        $identifier = 'honeybee_cmf.projection_fixtures.book-a726301d-dbae-4fb6-91e9-a19188a17e71-de_DE-1';

        $this->mockConnector->shouldReceive('getConfig')->twice()->andReturn(new ArrayConfig([]));
        $this->mockConnector->shouldReceive('getConnection')->once()->andReturn($this->mockClient);
        $this->mockClient->shouldReceive('get')->once()->with([
            'index' => 'index',
            'type' => null,
            'id' => $identifier
        ])->andReturn($testData['raw_result']);

        $projectionReader = new ProjectionReader(
            $this->mockConnector,
            new ArrayConfig(['index' => 'index']),
            new NullLogger,
            $this->projectionTypeMap
        );

        $projections = $this->createProjections([$testData['raw_result']]);
        $this->assertEquals($projections[0], $projectionReader->read($identifier));
    }

    /**
     * @expectedException Trellis\Common\Error\RuntimeException
     */
    public function testReadUnknownProjectionType()
    {
        $testData = include(__DIR__ . '/Fixture/projection_reader_test_03.php');
        $identifier = 'honeybee_cmf.projection_fixtures.book-a726301d-dbae-4fb6-91e9-a19188a17e71-de_DE-1';

        $this->mockConnector->shouldReceive('getConfig')->twice()->andReturn(new ArrayConfig([]));
        $this->mockConnector->shouldReceive('getConnection')->once()->andReturn($this->mockClient);
        $this->mockClient->shouldReceive('get')->once()->with([
            'index' => 'index',
            'type' => null,
            'id' => $identifier
        ])->andReturn($testData['invalid_result']);

        $projectionReader = new ProjectionReader(
            $this->mockConnector,
            new ArrayConfig(['index' => 'index']),
            new NullLogger,
            $this->projectionTypeMap
        );

        $projectionReader->read($identifier);
    }

    public function testReadMissing()
    {
        $testData = include(__DIR__ . '/Fixture/projection_reader_test_03.php');
        $this->mockConnector->shouldReceive('getConfig')->twice()->andReturn(new ArrayConfig([]));
        $this->mockConnector->shouldReceive('getConnection')->once()->andReturn($this->mockClient);
        $this->mockClient->shouldReceive('get')->once()->with([
            'index' => 'index',
            'type' => 'type1,type2',
            'id' => 'missing'
        ])->andThrow(Missing404Exception::CLASS);

        $projectionReader = new ProjectionReader(
            $this->mockConnector,
            new ArrayConfig(['index' => 'index', 'type' => 'type1,type2']),
            new NullLogger,
            $this->projectionTypeMap
        );

        $this->assertNull($projectionReader->read('missing'));
    }

    public function testGetIterator()
    {
        $testData = include(__DIR__ . '/Fixture/projection_reader_test_01.php');
        $this->mockConnector->shouldReceive('getConfig')->twice()->andReturn(new ArrayConfig([]));
        $this->mockConnector->shouldReceive('getConnection')->once()->andReturn($this->mockClient);
        $this->mockClient->shouldReceive('search')->once()->with([
            'index' => 'index',
            'type' => 'type',
            'size' => 10,
            'body' => ['query' => ['match_all' => []]]
        ])->andReturn($testData['raw_result']);

        $projectionReader = new ProjectionReader(
            $this->mockConnector,
            new ArrayConfig(['index' => 'index', 'type' => 'type']),
            new NullLogger,
            $this->projectionTypeMap
        );

        $iterator = $projectionReader->getIterator();
        $this->assertInstanceOf(StorageReaderIterator::CLASS, $iterator);
        $this->assertTrue($iterator->valid());
    }

    private function createProjections(array $results)
    {
        $projections = [];
        foreach ($results as $result) {
            $projections[] = $this->projectionTypeMap
                ->getItem($result['_source']['@type'])
                ->createEntity($result['_source']);
        }
        return $projections;
    }
}

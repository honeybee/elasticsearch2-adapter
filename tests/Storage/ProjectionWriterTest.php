<?php

namespace Honeybee\Tests\Elasticsearch2\Storage;

use Elasticsearch\Connections\ConnectionInterface;
use Honeybee\Elasticsearch2\Storage\Projection\ProjectionWriter;
use Honeybee\Infrastructure\Config\ArrayConfig;
use Honeybee\Infrastructure\Config\SettingsInterface;
use Honeybee\Infrastructure\DataAccess\Connector\ConnectorInterface;
use Honeybee\Projection\ProjectionInterface;
use Honeybee\Projection\ProjectionList;
use Honeybee\Projection\ProjectionMap;
use Honeybee\Tests\Elasticsearch2\TestCase;
use Mockery;
use Psr\Log\NullLogger;

class ProjectionWriterTest extends TestCase
{
    public function testWrite()
    {
        $expected = [
            'index' => 'test-index',
            'type' => 'test-type',
            'id' => 'test-identifier',
            'body' => [
                'test' => 'data',
                'sample' => ['value']
            ]
        ];

        $mockProjection = Mockery::mock(ProjectionInterface::CLASS);
        $mockProjection->shouldReceive('getIdentifier')->once()->withNoArgs()->andReturn($expected['id']);
        $mockProjection->shouldReceive('toArray')->once()->withNoArgs()->andReturn($expected['body']);

        $mockConnection = Mockery::mock(ConnectionInterface::CLASS);
        $mockConnection->shouldReceive('index')->once()->with($expected)->andReturnNull();

        $mockConfig = Mockery::mock(SettingsInterface::CLASS);
        $mockConfig->shouldReceive('get')->once()->with('index')->andReturn($expected['index']);
        $mockConfig->shouldReceive('get')->once()->with('type')->andReturn($expected['type']);

        $mockConnector = Mockery::mock(ConnectorInterface::CLASS);
        $mockConnector->shouldReceive('getConnection')->once()->withNoArgs()->andReturn($mockConnection);
        $mockConnector->shouldReceive('getConfig')->twice()->andReturn($mockConfig);

        $projectionWriter = new ProjectionWriter(
            $mockConnector,
            new ArrayConfig([]),
            new NullLogger
        );

        $this->assertNull($projectionWriter->write($mockProjection));
    }

    public function testWriteWithCustomConfig()
    {
        $expected = [
            'index' => 'override-index',
            'type' => 'overrid-type',
            'id' => 'test-identifier',
            'body' => [
                'test' => 'data',
                'sample' => ['value']
            ],
            'refresh' => true,
            'option' => 'value'
        ];

        $mockProjection = Mockery::mock(ProjectionInterface::CLASS);
        $mockProjection->shouldReceive('getIdentifier')->once()->withNoArgs()->andReturn($expected['id']);
        $mockProjection->shouldReceive('toArray')->once()->withNoArgs()->andReturn($expected['body']);

        $mockConnection = Mockery::mock(ConnectionInterface::CLASS);
        $mockConnection->shouldReceive('index')->once()->with($expected)->andReturnNull();

        $mockConfig = Mockery::mock(SettingsInterface::CLASS);
        $mockConfig->shouldReceive('get')->once()->with('index')->andReturn('test-index');
        $mockConfig->shouldReceive('get')->once()->with('type')->andReturn('test-type');

        $mockConnector = Mockery::mock(ConnectorInterface::CLASS);
        $mockConnector->shouldReceive('getConnection')->once()->withNoArgs()->andReturn($mockConnection);
        $mockConnector->shouldReceive('getConfig')->twice()->andReturn($mockConfig);

        $projectionWriter = new ProjectionWriter(
            $mockConnector,
            new ArrayConfig([
                'index' => $expected['index'],
                'type' => $expected['type'],
                'parameters' => [
                    'index' => ['refresh' => true, 'option' => 'value']
                ]
            ]),
            new NullLogger
        );

        $this->assertNull($projectionWriter->write($mockProjection));
    }

    /**
     * @expectedException Honeybee\Common\Error\RuntimeError
     */
    public function testWriteWithNull()
    {
        $mockConnector = Mockery::mock(ConnectorInterface::CLASS);
        $mockConnector->shouldNotReceive('getConnection');

        $projectionWriter = new ProjectionWriter(
            $mockConnector,
            new ArrayConfig([]),
            new NullLogger
        );

        $this->assertNull($projectionWriter->write(null));
    } //@codeCoverageIgnore

    /**
     * @expectedException Honeybee\Common\Error\RuntimeError
     */
    public function testWriteWithArray()
    {
        $mockConnector = Mockery::mock(ConnectorInterface::CLASS);
        $mockConnector->shouldNotReceive('getConnection');

        $projectionWriter = new ProjectionWriter(
            $mockConnector,
            new ArrayConfig([]),
            new NullLogger
        );

        $this->assertNull($projectionWriter->write(['data']));
    } //@codeCoverageIgnore

    public function testDelete()
    {
        $expected = [
            'index' => 'test-index',
            'type' => 'test-type',
            'id' => 'delete-identifier'
        ];

        $mockConnection = Mockery::mock(ConnectionInterface::CLASS);
        // ES create index on delete expectations
        $getData = $expected;
        $getData['refresh'] = false;
        $mockConnection->shouldReceive('get')->once()->with($getData)->andReturnNull();
        // end workaround expectations
        $mockConnection->shouldReceive('delete')->once()->with($expected)->andReturnNull();

        $mockConfig = Mockery::mock(SettingsInterface::CLASS);
        $mockConfig->shouldReceive('get')->once()->with('index')->andReturn($expected['index']);
        $mockConfig->shouldReceive('get')->once()->with('type')->andReturn($expected['type']);

        $mockConnector = Mockery::mock(ConnectorInterface::CLASS);
        $mockConnector->shouldReceive('getConnection')->once()->withNoArgs()->andReturn($mockConnection);
        $mockConnector->shouldReceive('getConfig')->twice()->andReturn($mockConfig);

        $projectionWriter = new ProjectionWriter(
            $mockConnector,
            new ArrayConfig([]),
            new NullLogger
        );

        $this->assertNull($projectionWriter->delete($expected['id']));
    }

    public function testDeleteWithCustomConfig()
    {
        $expected = [
            'index' => 'override-index',
            'type' => 'override-type',
            'id' => 'delete-identifier',
            'refresh' => true,
            'option' => 'value'
        ];

        $mockConnection = Mockery::mock(ConnectionInterface::CLASS);
        // ES create index on delete expectations
        $getData = $expected;
        $getData['refresh'] = false;
        unset($getData['option']);
        $mockConnection->shouldReceive('get')->once()->with($getData)->andReturnNull();
        // end workaround expectations
        $mockConnection->shouldReceive('delete')->once()->with($expected)->andReturnNull();

        $mockConfig = Mockery::mock(SettingsInterface::CLASS);
        $mockConfig->shouldReceive('get')->once()->with('index')->andReturn('test-index');
        $mockConfig->shouldReceive('get')->once()->with('type')->andReturn('test-type');

        $mockConnector = Mockery::mock(ConnectorInterface::CLASS);
        $mockConnector->shouldReceive('getConnection')->once()->withNoArgs()->andReturn($mockConnection);
        $mockConnector->shouldReceive('getConfig')->twice()->andReturn($mockConfig);

        $projectionWriter = new ProjectionWriter(
            $mockConnector,
            new ArrayConfig([
                'index' => 'override-index',
                'type' => 'override-type',
                'parameters' => [
                    'get' => ['refresh' => true],
                    'delete' => ['refresh' => true, 'option' => 'value']
                ]
            ]),
            new NullLogger
        );

        $this->assertNull($projectionWriter->delete($expected['id']));
    }

    public function testDeleteWithNull()
    {
        // Expected behaviour is to log and ignore invalid ids
        $mockConnector = Mockery::mock(ConnectorInterface::CLASS);
        $mockConnector->shouldNotReceive('getConnection');

        $projectionWriter = new ProjectionWriter(
            $mockConnector,
            new ArrayConfig([]),
            new NullLogger
        );

        $this->assertNull($projectionWriter->delete(null));
    }

    public function testDeleteWithArray()
    {
        // Expected behaviour is to log and ignore invalid ids
        $mockConnector = Mockery::mock(ConnectorInterface::CLASS);
        $mockConnector->shouldNotReceive('getConnection');

        $projectionWriter = new ProjectionWriter(
            $mockConnector,
            new ArrayConfig([]),
            new NullLogger
        );

        $this->assertNull($projectionWriter->delete(['id' => 'nope']));
    }

    public function testDeleteWithEmptyString()
    {
        // Expected behaviour is to log and ignore invalid ids
        $mockConnector = Mockery::mock(ConnectorInterface::CLASS);
        $mockConnector->shouldNotReceive('getConnection');

        $projectionWriter = new ProjectionWriter(
            $mockConnector,
            new ArrayConfig([]),
            new NullLogger
        );

        $this->assertNull($projectionWriter->delete(''));
    }

    public function testWriteMany()
    {
        $projections = [
            'k-l' => ['identifier' => 'k-l', 'firstname' => 'Konrad', 'lastname' => 'Lorenz'],
            'w-t' => ['identifier' => 'w-t', 'firstname' => 'Wilfred',  'lastname' => 'Thesiger']
        ];

        $mockMap = Mockery::mock(ProjectionMap::CLASS);
        $mockMap->shouldReceive('isEmpty')->once()->withNoArgs()->andReturn(false);
        $mockMap->shouldReceive('getSize')->once()->withNoArgs()->andReturn(2);
        $mockMap->shouldReceive('toArray')->once()->withNoArgs()->andReturn($projections);

        $expected = [
            'body' => [
                ['index' => ['_index' => 'test-index', '_type' => 'test-type', '_id' => 'k-l']],
                $projections['k-l'],
                ['index' => ['_index' => 'test-index', '_type' => 'test-type', '_id' => 'w-t']],
                $projections['w-t']
            ]
        ];

        $mockConnection = Mockery::mock(ConnectionInterface::CLASS);
        $mockConnection->shouldReceive('bulk')->once()
            ->with(Mockery::on(
                function (array $data) use ($expected) {
                    $this->assertEquals($expected, $data);
                    return true;
                }
            ))
            ->andReturnNull();

        $mockConfig = Mockery::mock(SettingsInterface::CLASS);
        $mockConfig->shouldReceive('get')->once()->with('index')->andReturn('test-index');
        $mockConfig->shouldReceive('get')->once()->with('type')->andReturn('test-type');

        $mockConnector = Mockery::mock(ConnectorInterface::CLASS);
        $mockConnector->shouldReceive('getConnection')->once()->withNoArgs()->andReturn($mockConnection);
        $mockConnector->shouldReceive('getConfig')->twice()->andReturn($mockConfig);

        $projectionWriter = new ProjectionWriter(
            $mockConnector,
            new ArrayConfig([]),
            new NullLogger
        );

        $this->assertNull($projectionWriter->writeMany($mockMap));
    }

    public function testWriteManyOneProjection()
    {
        $expected = [
            'index' => 'test-index',
            'type' => 'test-type',
            'id' => 'test-identifier',
            'body' => ['test' => 'data']
        ];

        $mockProjection = Mockery::mock(ProjectionInterface::CLASS);
        $mockProjection->shouldReceive('getIdentifier')->once()->withNoArgs()->andReturn($expected['id']);
        $mockProjection->shouldReceive('toArray')->once()->withNoArgs()->andReturn($expected['body']);

        // expectation is to redirect to write on single projection
        $mockMap = Mockery::mock(ProjectionMap::CLASS);
        $mockList = Mockery::mock(ProjectionList::CLASS);
        $mockMap->shouldReceive('isEmpty')->once()->withNoArgs()->andReturn(false);
        $mockMap->shouldReceive('getSize')->once()->withNoArgs()->andReturn(1);
        $mockMap->shouldReceive('toList')->once()->withNoArgs()->andReturn($mockList);
        $mockList->shouldReceive('getFirst')->once()->withNoArgs()->andReturn($mockProjection);

        $mockConnection = Mockery::mock(ConnectionInterface::CLASS);
        $mockConnection->shouldReceive('index')->once()->with($expected)->andReturnNull();

        $mockConfig = Mockery::mock(SettingsInterface::CLASS);
        $mockConfig->shouldReceive('get')->once()->with('index')->andReturn($expected['index']);
        $mockConfig->shouldReceive('get')->once()->with('type')->andReturn($expected['type']);

        $mockConnector = Mockery::mock(ConnectorInterface::CLASS);
        $mockConnector->shouldReceive('getConnection')->once()->withNoArgs()->andReturn($mockConnection);
        $mockConnector->shouldReceive('getConfig')->twice()->andReturn($mockConfig);

        $projectionWriter = new ProjectionWriter(
            $mockConnector,
            new ArrayConfig([]),
            new NullLogger
        );

        $this->assertNull($projectionWriter->writeMany($mockMap));
    }

    public function testWriteManyWithEmptyList()
    {
        $mockMap = Mockery::mock(ProjectionMap::CLASS);
        $mockMap->shouldReceive('isEmpty')->once()->withNoArgs()->andReturn(true);
        $mockMap->shouldNotReceive('toArray');

        $mockConnector = Mockery::mock(ConnectorInterface::CLASS);
        $mockConnector->shouldNotReceive('getConnection');

        $projectionWriter = new ProjectionWriter(
            $mockConnector,
            new ArrayConfig([]),
            new NullLogger
        );

        $this->assertNull($projectionWriter->writeMany($mockMap));
    }

    /**
     * @expectedException Honeybee\Common\Error\RuntimeError
     */
    public function testWriteManyWithNull()
    {
        $mockConnector = Mockery::mock(ConnectorInterface::CLASS);
        $mockConnector->shouldNotReceive('getConnection');

        $projectionWriter = new ProjectionWriter(
            $mockConnector,
            new ArrayConfig([]),
            new NullLogger
        );

        $this->assertNull($projectionWriter->writeMany(null));
    } //@codeCoverageIgnore

    /**
     * @expectedException Honeybee\Common\Error\RuntimeError
     */
    public function testWriteManyWithArray()
    {
        $mockConnector = Mockery::mock(ConnectorInterface::CLASS);
        $mockConnector->shouldNotReceive('getConnection');

        $projectionWriter = new ProjectionWriter(
            $mockConnector,
            new ArrayConfig([]),
            new NullLogger
        );

        $this->assertNull($projectionWriter->writeMany(['a', 'b']));
    } //@codeCoverageIgnore
}

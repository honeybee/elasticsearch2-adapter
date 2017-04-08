<?php

namespace Honeybee\Tests\Elasticsearch2\Storage\DomainEvent;

use Honeybee\Elasticsearch2\Storage\DomainEvent\DomainEventWriter;
use Honeybee\Infrastructure\Config\ArrayConfig;
use Honeybee\Infrastructure\DataAccess\Connector\ConnectorInterface;
use Honeybee\Tests\Elasticsearch2\TestCase;
use Mockery;
use Psr\Log\NullLogger;
use Honeybee\Model\Event\AggregateRootEventInterface;

class DomainEventWriterTest extends TestCase
{
    private $mockConnector;

    public function setUp()
    {
        $this->mockConnector = Mockery::mock(ConnectorInterface::CLASS);
    }

    /**
     * @expectedException Assert\InvalidArgumentException
     */
    public function testWriteInvalidDomainEvent()
    {
        $eventWriter = new DomainEventWriter($this->mockConnector, new ArrayConfig([]), new NullLogger);
        $eventWriter->write('');
    }

    public function testWrite()
    {
        $eventWriter = Mockery::mock(
            DomainEventWriter::CLASS.'[writeData]',
            [
                $this->mockConnector,
                new ArrayConfig([]),
                new NullLogger
            ]
        )->shouldAllowMockingProtectedMethods('writeData');
        $eventWriter->shouldReceive('writeData')->once()->with(
            'test_uuid',
            ['uuid' => 'test_uuid'],
            null
        )->andReturnNull();
        $mockEvent = Mockery::mock(AggregateRootEventInterface::CLASS);
        $mockEvent->shouldReceive('getUuid')->once()->withNoArgs()->andReturn('test_uuid');
        $mockEvent->shouldReceive('toArray')->once()->withNoArgs()->andReturn(['uuid' => 'test_uuid']);

        $eventWriter->write($mockEvent);
    }
}

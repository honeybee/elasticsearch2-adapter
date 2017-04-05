<?php

namespace Honeybee\Tests\Elasticsearch2\Fixture\BookSchema\Projection\Author;

use Honeybee\Tests\Elasticsearch2\Fixture\BookSchema\Projection\ProjectionType;
use Trellis\Runtime\Attribute\Email\EmailAttribute;
use Trellis\Runtime\Attribute\EmbeddedEntityList\EmbeddedEntityListAttribute;
use Trellis\Runtime\Attribute\EntityReferenceList\EntityReferenceListAttribute;
use Trellis\Runtime\Attribute\Text\TextAttribute;
use Trellis\Runtime\Attribute\Timestamp\TimestampAttribute;

class AuthorType extends ProjectionType
{
    public function __construct()
    {
        parent::__construct(
            'Author',
            [
                new TextAttribute('firstname', $this, ['mandatory' => true, 'min_length' => 2]),
                new TextAttribute('lastname', $this, ['mandatory' => true]),
                new EmailAttribute('email', $this, ['mandatory' => true]),
                new TimestampAttribute('birth_date', $this),
                new TextAttribute('blurb', $this, ['default_value' =>  'the grinch']),
                new EmbeddedEntityListAttribute(
                    'products',
                    $this,
                    [
                        'inline_mode' => true,
                        'entity_types' => [
                            __NAMESPACE__.'\\Embed\\HighlightType',
                        ]
                    ]
                ),
                new EntityReferenceListAttribute(
                    'books',
                    $this,
                    [
                        'inline_mode' => true,
                        'entity_types' => [
                            __NAMESPACE__.'\\Reference\\BookType',
                        ]
                    ]
                )
            ]
        );
    }

    public static function getEntityImplementor()
    {
        return Author::CLASS;
    }
}

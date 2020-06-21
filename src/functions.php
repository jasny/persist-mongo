<?php

declare(strict_types=1);

namespace Jasny\DB\Mongo;

use Improved as i;

/**
 * Flatten all fields of an element.
 * @internal
 *
 * @param mixed  $element
 * @param string $path
 * @param array  $accumulator  Don't use
 * @return array
 */
function flatten_fields($element, string $path = '', array &$accumulator = []): array
{
    if (!is_array($element) && !is_object($element)) {
        $accumulator[$path] = $element;
    } else {
        foreach ($element as $key => $value) {
            i\type_check($key, 'string', new \UnexpectedValueException());

            $field = ($path === '' ? $key : "$path.$key");
            flatten_fields($value, $field, $accumulator); // recursion
        }
    }

    return $accumulator;
}

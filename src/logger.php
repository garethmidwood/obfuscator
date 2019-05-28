<?php

class Logger 
{
    public function errorMessage(string $message, $die = false)
    {
        echo "   ✗ $message" . PHP_EOL;

        if ($die) {
            exit();
        }
    }

    public function progressMessage(string $message)
    {
        echo "• $message" . PHP_EOL;
    }

    public function completeMessage(string $message)
    {
        echo "   ✓ $message" . PHP_EOL;
    }
}

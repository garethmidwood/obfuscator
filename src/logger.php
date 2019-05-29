<?php

class Logger 
{
    public function errorMessage(string $message, $die = false)
    {
        echo date('H:i:s') . "      ✗ $message" . PHP_EOL;

        if ($die) {
            exit();
        }
    }

    public function progressMessage(string $message)
    {
        echo date('H:i:s') . "   • $message" . PHP_EOL;
    }

    public function completeMessage(string $message)
    {
        echo date('H:i:s') . "      ✓ $message" . PHP_EOL;
    }
}

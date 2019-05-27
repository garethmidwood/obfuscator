<?php

include_once('./vendor/autoload.php');

include_once('./logger.php');
include_once('./obfuscator.php');
include_once('./source.php');

use Symfony\Component\Yaml\Yaml;



// TODO: Move this into a config class that validates itself
$config = Yaml::parseFile('obfuscate.yml');

if (!isset($config['database'])) {
    exit('database node is required');
}

if (!isset($config['database']['user'])) {
    exit('database user node is required');
}

if (!isset($config['database']['password'])) {
    exit('database password node is required');
}

if (!isset($config['database']['host'])) {
    exit('database host node is required');
}

if (!isset($config['database']['port'])) {
    exit('database port node is required');
}





// check sources are defined and correctly configured
if (!isset($config['source'])) {
    exit('source node is required');
}

if (!is_array($config['source'])) {
    exit('source node must be an array');
}






if (isset($config['dryrun']) && $config['dryrun'] == true) {
    progressMessage('========================');
    progressMessage('======== DRY RUN =======');
    progressMessage('========================');
    define('DRY_RUN', true);
} else {
    define('DRY_RUN', false);
}


/**
 * Logger
 */
$logger = new Logger();




/**
 * DB Connection setup
 */
$dbConnection = new mysqli(
    $config['database']['host'] . ':' . $config['database']['port'],
    $config['database']['user'],
    $config['database']['password']
);

if ($dbConnection->connect_errno) {
    $logger->errorMessage('Database connection failed: ' . $dbConnection->connect_error, true);
}

$logger->progressMessage('Setting global database config');

$sql = 'SET @@global.max_allowed_packet = 524288000';
if (!$dbConnection->multi_query($sql)) {
    $logger->errorMessage('Could not run sql ' . $dbConnection->error . PHP_EOL . PHP_EOL . $sql, true);
}

$logger->completeMessage('Set max_allowed_packet on DB');


/**
 * Local file storage location setup
 */
$storageDir = sys_get_temp_dir() . DIRECTORY_SEPARATOR . 'obfuscation' . DIRECTORY_SEPARATOR;

$sources = [];
foreach($config['source'] as $source) {
    $sources[] = new Source($source, $storageDir, $dbConnection, $logger);
}

/**
 * Run the obfuscation on each source
 */
$ob = new Obfuscator($config, $sources);
$ob->run();

<?php

include_once('./vendor/autoload.php');

use Symfony\Component\Yaml\Yaml;

define('DB_FILE', sys_get_temp_dir() . DIRECTORY_SEPARATOR . 'sql.sql');
define('MANIFEST_FILE', sys_get_temp_dir() . DIRECTORY_SEPARATOR . 'manifest.yml');
define('TMP_DB_NAME', 'derek');

// TODO: Move this into a config class that validates itself
$config = Yaml::parseFile('obfuscate.yml');

if (!isset($config['source'])) {
    exit('source node is required');
}

if (!is_array($config['source'])) {
    exit('source node must be an array');
}

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

$dbConnection = new mysqli(
        $config['database']['host'] . ':' . $config['database']['port'],
        $config['database']['user'],
        $config['database']['password']
    );

if ($dbConnection->connect_errno) {
    errorMessage('Database connection failed: ' . $dbConnection->connect_error, true);
}


// TODO: Move this into a task runner class
foreach($config['source'] as $source) {
    // create AWS client
    $client = new \Aws\S3\S3Client([
        'region'  => $source['region'],
        'version' => 'latest',
        'credentials' => [
            'key'    => $source['access'],
            'secret' => $source['secret'],
        ],
    ]);

    $aPairedS3Objects = s3Iterate($client, $source);

    processObfuscation($client, $aPairedS3Objects, $source, $dbConnection);
}

function s3Iterate(\Aws\S3\S3Client $s3client, array $source) {
    $objects = $s3client->getIterator('ListObjects', array(
        'Bucket' => $source['bucket']
    ));

    $pairedObjects = [];

    foreach ($objects as $object) {
        pairS3Objects($object, $pairedObjects);
    }

    return $pairedObjects;
}

function pairS3Objects(array $s3object, array &$pairedObjects) {
    $ext = pathinfo($s3object['Key'], PATHINFO_EXTENSION);

    $pairedObjects[dirname($s3object['Key'])][$ext] = basename($s3object['Key']);
} 

function processObfuscation(\Aws\S3\S3Client $s3client, array $pairedObjects, array $source, mysqli $dbConnection) {

    foreach($pairedObjects as $path => $pair) {
        if (!isset($pair['sql'])) {
            errorMessage("skipping $path as there is no sql file");
        }

        if (!isset($pair['yml'])) {
            errorMessage("skipping $path as there is no yml file");
        }

        progressMessage('========================');
        progressMessage('Processing ' . $path);

        try {
            // get the manifest
            $result = $s3client->getObject([
                'Bucket'     => $source['bucket'],
                'Key'        => $path . DIRECTORY_SEPARATOR . $pair['yml'],
                'SaveAs'     => MANIFEST_FILE,
            ]);

            progressMessage('✓ Downloaded manifest file');

            // get the sql
            $result = $s3client->getObject([
                'Bucket'     => $source['bucket'],
                'Key'        => $path . DIRECTORY_SEPARATOR . $pair['sql'],
                'SaveAs'     => DB_FILE,
            ]);

            progressMessage('✓ Downloaded sql file');

            if (!$dbConnection->query('CREATE DATABASE ' . TMP_DB_NAME)) {
                throw new \Exception('DB Error message: ' . $dbConnection->error);
            }

            progressMessage('✓ Created database ' . TMP_DB_NAME);

            if (!$dbConnection->select_db(TMP_DB_NAME)) {
                throw new \Exception('DB Error message: Can\'t select database ' . TMP_DB_NAME);
            }

            progressMessage('✓ Selected database');

            $sql = file_get_contents(DB_FILE);

            if (!$dbConnection->multi_query($sql)) {
                throw new \Exception('DB Error message: ' . $dbConnection->error);
            }

            progressMessage('✓ Imported DB dump');

            do {
                /* store first result set */
                if ($result = $dbConnection->store_result()) {
                    while ($row = $result->fetch_row()) {
                        printf("%s\n", $row[0]);
                    }
                    $result->free();
                }

                $dbConnection->next_result();
            } while ($dbConnection->more_results());

            progressMessage('✓ Freed import results');

            $manifest = Yaml::parseFile(MANIFEST_FILE);

            progressMessage('✓ Parsed manifest.yml');

            var_dump($manifest);


            // delete the manifest from S3, but not the DB dump


        } catch(\Exception $e) {
            // raise error, but keep processing
            errorMessage($e->getMessage());
        }
    }

    cleanUpFiles();
    cleanUpDatabase($dbConnection);

    progressMessage('========================');
}

function progressMessage(string $message) {
    echo $message . PHP_EOL;
}

function errorMessage(string $message, $die = false) {
    echo $message . PHP_EOL;

    if ($die) {
        exit();
    }
}

function cleanUpFiles() {
    progressMessage('✓ Cleaning up files');
    if (file_exists(DB_FILE)) {
        unlink(DB_FILE);
    }
    if (file_exists(MANIFEST_FILE)) {
        unlink(MANIFEST_FILE);
    }
}

function cleanUpDatabase(mysqli $dbConnection) {
    progressMessage('✓ Cleaning up database');
    if (!$dbConnection->query('DROP DATABASE ' . TMP_DB_NAME)) {
        errorMessage('DB Error message: ' . $dbConnection->error);
    }
}

echo PHP_EOL;

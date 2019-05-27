<?php

use Aws\S3\S3Client;
use Symfony\Component\Yaml\Yaml;

class Source
{
    const MANIFEST_FILE = 'manifest.yml';
    /**
     * @var string
     */
    private $type,
            $bucket,
            $region,
            $access,
            $secret,
            $directory;

    /**
     * @var S3Client
     */
    private $client;

    /**
     * @var array
     */
    private $sqlFiles = [];

    /**
     * @var string
     */
    private $manifestFile;

    /**
     * @var Logger
     */
    private $logger;
    
    /**
     * @var string
     */
    private $storageDir;
    private $partsDir = 'parts';

    /**
     * @var mysqli
     */
    private $dbConnection;
    private $obfuscationDbName = 'tmp_obfuscation';

    /**
     * Constructor
     * @param array $source 
     * @param string $storageDir
     * @param mysqli $dbConnection
     * @param Logger $logger
     * @return void
     */
    public function __construct(
        array $source,
        string $storageDir,
        mysqli $dbConnection,
        Logger $logger
    ) {
        if (!isset($source['type'])) {
            exit('source type node is required');
        }

        $this->type = $source['type'];

        if (!isset($source['bucket'])) {
            exit('source bucket node is required');
        }

        $this->bucket = $source['bucket'];

        if (!isset($source['region'])) {
            exit('source region node is required');
        }

        $this->region = $source['region'];

        if (!isset($source['access'])) {
            exit('source access node is required');
        }

        $this->access = $source['access'];

        if (!isset($source['secret'])) {
            exit('source secret node is required');
        }

        $this->secret = $source['secret'];

        $this->directory = isset($source['directory']) ? $source['directory'] : '';

        $this->storageDir = $storageDir . $this->bucket . DIRECTORY_SEPARATOR;

        $this->logger = $logger;

        $this->dbConnection = $dbConnection;

        $this->createClient();
    }

    /**
     * Connects to S3
     * @return type
     */
    private function createClient() 
    {
        $this->client = new S3Client([
            'region'  => $this->region,
            'version' => 'latest',
            'credentials' => [
                'key'    => $this->access,
                'secret' => $this->secret,
            ],
        ]);
    }

    /**
     * Runs a query against the local DB
     * @param string $sql 
     * @return void
     */
    private function runDbQuery($sql) 
    {
        while ($this->dbConnection->more_results()
            && $this->dbConnection->next_result()
        ) {
            ; // flush multi_queries
        }

        if (!$this->dbConnection->multi_query($sql)) {
            $this->logger->errorMessage(
                'Could not run sql ' . $this->dbConnection->error . PHP_EOL . PHP_EOL . $sql, true
            );
        }
    }

    /**
     * Gathers details of the manifest and sql files to process
     * @return void
     */
    private function gatherFileDetails()
    {
        $params = array(
            'Bucket' => $this->bucket,
            'Prefix' => $this->directory
        );

        $objects = $this->client->getIterator('ListObjects', $params);

        foreach ($objects as $object) {
            $pathinfo = pathinfo($object['Key']);
            $filename = $pathinfo['dirname'] . DIRECTORY_SEPARATOR . $pathinfo['basename'];

            if ($pathinfo['basename'] == self::MANIFEST_FILE) {
                $this->manifestFile = $filename;
            } elseif ($pathinfo['extension'] == 'sql') {
                $this->sqlFiles[] = $filename;
            } else {
                $this->logger->errorMessage('unrecognised file: ' . $pathinfo['basename']);
            }
        }
    }

    /**
     * Checks that the obfuscation is safe to run
     * @return boolean
     */
    private function runPreObfuscationChecks() 
    {   
        // check source files exist
        if (!count($this->sqlFiles)) {
            $this->logger->errorMessage('no sql file');

            return false;
        }

        if (!isset($this->manifestFile)) {
            $this->logger->errorMessage('no manifest.yml file');

            return false;
        }

        // check local files can be written to storage dir
        if (!is_dir($this->storageDir)) {
            if (!mkdir($this->storageDir, 0777, true)) {
                $this->logger->errorMessage('Could not create storage dir ' . $this->storageDir);
                
                return false;
            }
        } elseif (!is_writable($this->storageDir)) {
            $this->logger->errorMessage('Could not write to storage dir ' . $this->storageDir);
            
            return false;
        }

        if (!is_dir($this->storageDir . $this->partsDir)) {
            if (!mkdir($this->storageDir . $this->partsDir, 0777, true)) {
                $this->logger->errorMessage('Could not create storage parts dir ' . $this->storageDir . $this->partsDir);
                
                return false;
            }
        } elseif (!is_writable($this->storageDir . $this->partsDir)) {
            $this->logger->errorMessage('Could not write to storage parts dir ' . $this->storageDir . $this->partsDir);
            
            return false;
        }

        return true;
    }

    /**
     * Clears local storage dirs to prevent re-use of old files
     * @return void
     */
    private function emptyStorageDirs()
    {
        $this->logger->progressMessage('Emptying local storage directory');
        $this->emptyStorageDir($this->storageDir);

        $this->logger->progressMessage('Emptying local storage parts directory');
        $this->emptyStorageDir($this->storageDir . $this->partsDir);
    }

    /**
     * Emptys a single dir of files
     * @param string $dir 
     * @return void
     */
    private function emptyStorageDir(string $dir)
    {
        $files = glob($dir . '*'); // get all file names

        foreach($files as $file){ // iterate files
            if(is_file($file)) {
                $this->logger->completeMessage('Removing ' . $file);
                unlink($file); // delete file
            }
        }

        $this->logger->completeMessage('Emptied local directory ' . $dir);
    }

    /**
     * Prepares the obfuscation DB
     * @return void
     */
    private function prepObfuscationDb()
    {
        $this->logger->progressMessage('Preparing obfuscation database');

        $this->runDbQuery('DROP DATABASE IF EXISTS ' . $this->obfuscationDbName);
        
        $this->logger->completeMessage('Removed database ' . $this->obfuscationDbName); 

        $this->runDbQuery('CREATE DATABASE ' . $this->obfuscationDbName);

        $this->logger->completeMessage('Created database ' . $this->obfuscationDbName); 

        if (!$this->dbConnection->select_db($this->obfuscationDbName)) {
            throw new \Exception('DB Error message: Can\'t select database ' . $this->obfuscationDbName);
        }

        $this->logger->completeMessage('Selected database');

        $this->runDbQuery('SET FOREIGN_KEY_CHECKS=0;');
        
        $this->logger->completeMessage('Turned off foreign key checks');
    }

    /**
     * Download and parse manifest file
     * @return void
     */
    private function downloadManifestFile()
    {
        $this->logger->progressMessage('Downloading manifest file ' . $this->manifestFile);

        $result = $this->client->getObject([
            'Bucket'     => $this->bucket,
            'Key'        => $this->manifestFile,
            'SaveAs'     => $this->storageDir . self::MANIFEST_FILE,
        ]);

        $this->logger->completeMessage('Downloaded manifest file');

        $manifest = Yaml::parseFile($this->storageDir . self::MANIFEST_FILE);

        $this->logger->completeMessage('Parsed manifest.yml');

        if (!isset($manifest['destination'])) {
            $this->logger->errorMessage('destination node is required', true);
        }

        if (!is_array($manifest['destination'])) {
            $this->logger->errorMessage('destination node must be an array', true);
        }

        if (!isset($manifest['destination']['type'])) {
            $this->logger->errorMessage('destination type node is required', true);
        }

        if (!isset($manifest['destination']['bucket'])) {
            $this->logger->errorMessage('destination bucket node is required', true);
        }

        if (!isset($manifest['destination']['region'])) {
            $this->logger->errorMessage('destination region node is required', true);
        }

        if (!isset($manifest['destination']['access'])) {
            $this->logger->errorMessage('destination access node is required', true);
        }

        if (!isset($manifest['destination']['secret'])) {
            $this->logger->errorMessage('destination secret node is required', true);
        }

        if (!isset($manifest['destination']['dir'])) {
            $this->logger->errorMessage('destination dir node is required', true);
        }

        if (!isset($manifest['destination']['structure_filename'])) {
            $this->logger->errorMessage('destination structure_filename node is required', true);
        }

        if (!isset($manifest['destination']['data_filename'])) {
            $this->logger->errorMessage('destination data_filename node is required', true);
        }

        $this->logger->completeMessage('Manifest file passed checks');

        // if (DRY_RUN) {
        //     foreach($manifest['data'] as $database) {
        //         foreach ($database as $tableName => $table) {
        //             foreach($table as $obfuscationType => $fields) {
        //                 obfuscateField($dbConnection, $tableName, $obfuscationType, $fields, DRY_RUN);
        //             }

        //             progressMessage('✓ Completed obfuscation of ' . $tableName);
        //         }
        //     }
        //     continue;
        // }
        
    }

    /**
     * Download and process sql file
     * @return void
     */
    private function downloadSqlFiles() 
    {
        $this->logger->progressMessage('Downloading sql files');
        
        foreach ($this->sqlFiles as $sqlFile) {
            $this->logger->progressMessage('Downloading sql file: ' . $sqlFile);

            $this->client->getObject([
                'Bucket'     => $this->bucket,
                'Key'        => $sqlFile,
                'SaveAs'     => $this->storageDir . pathinfo($sqlFile, PATHINFO_BASENAME)
            ]);

            $this->logger->completeMessage('File download complete');
        }

        $this->logger->completeMessage('SQL file downloads complete');
    }

    public function processObfuscation()
    {
        $identifier = $this->bucket . DIRECTORY_SEPARATOR . $this->directory;

        $this->gatherFileDetails();

        if (!$this->runPreObfuscationChecks()) {
            $this->logger->errorMessage(
                'Source did not pass pre-obfuscation checks, skipping ' . $identifier
            );
        }

        $this->emptyStorageDirs();
        $this->prepObfuscationDb();

        $this->logger->progressMessage('Processing ' . $identifier);

        $this->downloadManifestFile();
        $this->downloadSqlFiles();
    }

}









function processObfuscation(
    \Aws\S3\S3Client $sourceClient, array $pairedObjects, array $source, mysqli $dbConnection, array $dbConnectionDetails) {

    foreach($pairedObjects as $path => $pair) {
        

        try {



            while ($dbConnection->next_result()) {;} // flush multi_queries

            $sql = file_get_contents(STORAGE_DIR . SQL_STRUCTURE_FILE);
            if (!$dbConnection->multi_query($sql)) {
                throw new \Exception('DB Error message: ' . $dbConnection->error . ' (creating table structure)');
            }

            progressMessage('✓ Created DB structure'); 

            // split file into smaller parts so we can avoid importing a huge file
            exec("split --lines=1000 " . STORAGE_DIR . SQL_DATA_FILE . " " . STORAGE_PARTS_DIR);

            $dbPartFiles = array_diff(scandir(STORAGE_PARTS_DIR), array('..', '.'));

            progressMessage('✓ DB dump has been split into ' . count($dbPartFiles) . ' parts');

            foreach ($dbPartFiles as $filename) {
                $sql = file_get_contents(STORAGE_PARTS_DIR . $filename);

                while ($dbConnection->next_result()) {;} // flush multi_queries

                if (!$dbConnection->multi_query($sql)) {
                    throw new \Exception('DB Error message: ' . $dbConnection->error . ' when importing db part ' . $filename);
                }

                progressMessage('✓ Imported DB part ' . $filename);
            }

            progressMessage('✓ Imported DB dump');


            foreach($manifest['data'] as $database) {
                foreach ($database as $tableName => $table) {
                    foreach($table as $obfuscationType => $fields) {
                        obfuscateField($dbConnection, $tableName, $obfuscationType, $fields);
                    }

                    progressMessage('✓ Completed obfuscation of ' . $tableName);
                }
            }





            // create AWS client for pushing to destination
            $destinationClient = new \Aws\S3\S3Client([
                'region'  => $manifest['destination']['region'],
                'version' => 'latest',
                'credentials' => [
                    'key'    => $manifest['destination']['access'],
                    'secret' => $manifest['destination']['secret'],
                ],
            ]);

            progressMessage('✓ Connected to destination');





            // dump the DB data file locally
            $dump = new IMysqldump\Mysqldump(
                'mysql:host=' . $dbConnectionDetails['host'] . ':' . $dbConnectionDetails['port'] . ';dbname=' . $databaseName,
                $dbConnectionDetails['user'],
                $dbConnectionDetails['password'],
                [
                    'add-drop-table' => true,
                    'no-create-info' => true
                ]
            );
            $dump->start(CLEANSED_DB_FILE);

            progressMessage('✓ Dumped cleansed database');

            // push the data file to storage
            $result = $destinationClient->putObject([
                'Bucket'     => $manifest['destination']['bucket'],
                'Key'        => $manifest['destination']['dir'] . DIRECTORY_SEPARATOR . $manifest['destination']['data_filename'],
                'SourceFile' => CLEANSED_DB_FILE,
            ]);

            progressMessage('✓ Pushed cleansed data DB to destination');

            // push the structure file to storage
            $result = $destinationClient->putObject([
                'Bucket'     => $manifest['destination']['bucket'],
                'Key'        => $manifest['destination']['dir'] . DIRECTORY_SEPARATOR . $manifest['destination']['structure_filename'],
                'SourceFile' => STORAGE_DIR . SQL_STRUCTURE_FILE,
            ]);

            progressMessage('✓ Pushed cleansed DB to destination');





















            // delete the manifest from S3, but not the DB dumps
            $result = $sourceClient->deleteObject([
                'Bucket'     => $source['bucket'],
                'Key'        => $path . DIRECTORY_SEPARATOR . $pair['yml']
            ]);

            progressMessage('✓ Deleted manifest from source');

        } catch(\Exception $e) {
            // raise error, but keep processing
            errorMessage($e->getMessage());
        }

        while ($dbConnection->next_result()) {;} // flush multi_queries

        cleanUpFiles();
        cleanUpDatabase($dbConnection, $databaseName);

        progressMessage('========================');
    }
}

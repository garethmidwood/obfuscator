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
    private $sourceSqlFiles = [];

    /**
     * @var string
     */
    private $sourceManifestFile;

    /**
     * @var Logger
     */
    private $logger;
    
    /**
     * @var string
     */
    private $storageDir;
    private $partsDir = 'parts/';

    /**
     * @var obfuscationDb
     */
    private $db;
    private $obfuscationDbName = 'tmp_obfuscation';

    /**
     * Constructor
     * @param array $source 
     * @param string $storageDir
     * @param obfuscationDb $db
     * @param Logger $logger
     * @return void
     */
    public function __construct(
        array $source,
        string $storageDir,
        obfuscationDb $db,
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

        $this->db = $db;

        $this->client = $this->createClient($this->region, $this->access, $this->secret);
    }

    /**
     * Connects to S3
     * @param string $region 
     * @param string $access 
     * @param string $secret 
     * @return S3Client
     */
    private function createClient(
        string $region,
        string $access,
        string $secret
    ) {
        return new S3Client([
            'region'  => $region,
            'version' => 'latest',
            'credentials' => [
                'key'    => $access,
                'secret' => $secret,
            ],
        ]);
    }

    /**
     * Gathers details of the manifest and sql files to process
     * @param string $directory
     * @return void
     */
    private function gatherFileDetails($directory)
    {
        $this->logger->progressMessage('Gathering file details for ' . $directory);

        $params = array(
            'Bucket' => $this->bucket,
            'Prefix' => $directory
        );

        $objects = $this->client->getIterator('ListObjects', $params);

        // reset
        $this->sourceManifestFile = null;
        $this->sourceSqlFiles = [];

        foreach ($objects as $object) {
            $pathinfo = pathinfo($object['Key']);
            $filename = $pathinfo['dirname'] . DIRECTORY_SEPARATOR . $pathinfo['basename'];

            if ($pathinfo['basename'] == self::MANIFEST_FILE) {
                $this->sourceManifestFile = $filename;
            } elseif ($pathinfo['extension'] == 'sql') {
                $this->sourceSqlFiles[] = $filename;
            } else {
                $this->logger->errorMessage('unrecognised file: ' . $pathinfo['basename']);
            }
        }

        $this->logger->completeMessage('Gathered file details');
    }

    /**
     * Checks that the obfuscation is safe to run
     * @return boolean
     */
    private function runPreObfuscationChecks() 
    {   
        // check source files exist
        if (!count($this->sourceSqlFiles)) {
            $this->logger->errorMessage('no sql file');

            return false;
        }

        if (!isset($this->sourceManifestFile)) {
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

        $this->db->run('DROP DATABASE IF EXISTS ' . $this->obfuscationDbName);
        
        $this->logger->completeMessage('Removed database ' . $this->obfuscationDbName); 

        $this->db->run('CREATE DATABASE ' . $this->obfuscationDbName);

        $this->logger->completeMessage('Created database ' . $this->obfuscationDbName); 

        $this->db->selectDb($this->obfuscationDbName);

        $this->logger->completeMessage('Selected database');

        $this->db->run('SET FOREIGN_KEY_CHECKS=0;');
        
        $this->logger->completeMessage('Turned off foreign key checks');
    }

    /**
     * Download and parse manifest file
     * @return void
     */
    private function downloadManifestFile()
    {
        if (!isset($this->sourceManifestFile)) {
            throw new \Exception('Manifest file was not found');
        }

        $this->logger->progressMessage('Downloading manifest file ' . $this->sourceManifestFile);

        $result = $this->client->getObject([
            'Bucket'     => $this->bucket,
            'Key'        => $this->sourceManifestFile,
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

        $this->manifest = $manifest;

        $this->logger->completeMessage('Manifest file passed checks');        
    }

    /**
     * Download and process sql file
     * @return void
     */
    private function downloadSqlFiles() 
    {
        $this->logger->progressMessage('Downloading sql files');
        
        foreach ($this->sourceSqlFiles as $sqlFile) {
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

    /**
     * Imports the database files
     * @return void
     */
    private function importDbs()
    {
        $files = glob($this->storageDir . '*.sql'); // get all sql file names

        foreach($files as $sqlFile) {
            $this->logger->progressMessage('Importing ' . basename($sqlFile));
            
            if (filesize($sqlFile) > 100000000) { // 500 MB
                $this->splitAndImportFile($sqlFile);
            } else {
                $this->importDbFile($sqlFile);
            }

            $this->logger->completeMessage('Import complete'); 
        }
    }
    
    /**
     * Import file to DB
     * @param string $sqlFile 
     * @return void
     */
    private function importDbFile($sqlFile)
    {
        if (!file_exists($sqlFile)) {
            throw new \Exception("SQL file $sqlFile does not exist");
        }

        try {
            $sql = file_get_contents($sqlFile);
            
            $this->db->run($sql);

            unset($sql);
        } catch (\Exception $e) {
            $this->logger->errorMessage($e->getMessage(), true);
        }
    }

    /**
     * Splits a large DB file into sections and imports each one
     * @param string $sqlFile 
     * @return void
     */
    private function splitAndImportFile($sqlFile)
    {
        $this->logger->completeMessage('Splitting file @ ' . date('H:i:s'));
        // split file into smaller parts so we can avoid importing a huge file
        exec("split --lines=2000000 " . $sqlFile . " " . $this->storageDir . $this->partsDir);

        $dbPartFiles = array_values(array_diff(scandir($this->storageDir . $this->partsDir), array('..', '.')));

        $totalFiles = count($dbPartFiles);

        $this->logger->completeMessage('DB dump has been split into ' . $totalFiles . ' parts  @ ' . date('H:i:s'));

        foreach ($dbPartFiles as $index => $filename) {
            $this->importDbFile($this->storageDir . $this->partsDir . $filename);

            $this->logger->completeMessage("Imported DB part ($index of $totalFiles) " . $filename . ' @ ' . date('H:i:s'));
        }

        $this->logger->completeMessage('Completed parts import. Emptying local storage parts directory');
        $this->emptyStorageDir($this->storageDir . $this->partsDir);
    }

    /**
     * Obfuscates the fields in the DB
     * @return void
     */
    private function obfuscate()
    {
        foreach($this->manifest['data'] as $database) {
            foreach ($database as $tableName => $table) {
                foreach($table as $obfuscationType => $fields) {
                    $this->obfuscateFieldsByType($tableName, $obfuscationType, $fields);
                }

                $this->logger->completeMessage('Completed obfuscation of ' . $tableName);
            }
        }
    }

    /**
     * Obfuscates fields of a specific type
     * @param string $tableName 
     * @param string $obfuscationType 
     * @param array $fields 
     * @return void
     */
    private function obfuscateFieldsByType (
        string $tableName,
        string $obfuscationType,
        array $fields
    ) {
        $fieldUpdates = [];

        foreach($fields as $field) {
            switch($obfuscationType) {
                case 'email':
                    $fieldUpdates[] = "`$field` = concat(LEFT(UUID(), 8), '@example.com')";
                    break;
                case 'date':
                    $fieldUpdates[] = "`$field` = CURDATE()";
                    break;
                case 'string':
                    $fieldUpdates[] = "`$field` = LEFT(UUID(), 10)";
                    break;
                case 'float':
                    $fieldUpdates[] = "`$field` = '1.0'";
                    break;
                case 'bigint':
                    $fieldUpdates[] = "`$field` = '12345'";
                    break;
                case 'int':
                    $fieldUpdates[] = "`$field` = '10'";
                    break;
                case 'phone':
                    $fieldUpdates[] = "`$field` = '01234567890'";
                    break;
                default:
                    $this->logger->completeMessage("Unrecognised field type $obfuscationType for `$field` - skipping");
                    break;
            }
        }

        $query = 'UPDATE ' . $tableName . ' set ' . implode(',', $fieldUpdates) . ' WHERE 1=1';

        $this->db->run($query);

        $this->logger->completeMessage("Obfuscated $obfuscationType fields in $tableName");
    }

    /**
     * Dumps the obfuscated database
     * @return void
     */
    private function dumpDb()
    {
        $this->logger->progressMessage('Dumping database');

        $outfile = $this->storageDir . '99_obfuscated_data.sql';

        $this->db->dumpDbData($this->obfuscationDbName, $outfile);

        $this->logger->completeMessage('Dumped cleansed database');
    }

    /**
     * Pushes the obfuscated DB to storage location
     * @return void
     */
    private function pushDumpToDestination()
    {
        $this->logger->progressMessage('Pushing dump to storage destination');

        $destinationClient = $this->createClient(
            $this->manifest['destination']['region'],
            $this->manifest['destination']['access'],
            $this->manifest['destination']['secret']
        );

        $this->logger->completeMessage('Connected to destination');        

        $localDumpFile = $this->storageDir . '99_obfuscated_data.sql';

        // push the data file to storage
        $result = $destinationClient->putObject([
            'Bucket'     => $this->manifest['destination']['bucket'],
            'Key'        => $this->manifest['destination']['dir'] . DIRECTORY_SEPARATOR . $this->manifest['destination']['data_filename'],
            'SourceFile' => $localDumpFile
        ]);

        $this->logger->completeMessage('Pushed cleansed data DB to destination');

        $localDbStructureFile = $this->storageDir . '03_structure_obfuscated.sql';

        // push the structure file to storage
        $result = $destinationClient->putObject([
            'Bucket'     => $this->manifest['destination']['bucket'],
            'Key'        => $this->manifest['destination']['dir'] . DIRECTORY_SEPARATOR . $this->manifest['destination']['structure_filename'],
            'SourceFile' => $localDbStructureFile,
        ]);

        $this->logger->completeMessage('Pushed cleansed DB structure to destination');

        $this->logger->completeMessage('Pushed all DB parts to destination');
    }

    /**
     * Deletes the manifest file from the bucket after processing
     * @return void
     */
    private function deleteManifest()
    {
        $this->logger->progressMessage('Removing manifest from source');

        // delete the manifest from S3, but not the DB dumps
        $result = $this->client->deleteObject([
            'Bucket'     => $this->bucket,
            'Key'        => $this->sourceManifestFile
        ]);

        $this->logger->completeMessage('Deleted manifest from source');
    }

    /**
     * Retrieves a list of the directories in the root of the bucket
     * @return array
     */
    private function getBucketDirectories()
    {
        $directories = [];

        $params = array(
            'Bucket' => $this->bucket
        );

        $objects = $this->client->getIterator('ListObjects', $params);

        foreach ($objects as $object) {
            $pathparts = explode('/', $object['Key']);
            $dir = $pathparts[0];

            if (!in_array($dir, $directories)) {
                $directories[] = $dir;
            }
        }

        return $directories;
    }

    /**
     * runs the obfuscation
     * @return void
     */
    public function processObfuscation()
    {
        $directories = [];

        // if there's no set directory then we need to scan the root and work through each dir individually
        if ($this->directory == '') {
            $directories = $this->getBucketDirectories();
        } else {
            $directories[] = $this->directory;
        }

        foreach ($directories as $directory) {
            $identifier = $this->bucket . DIRECTORY_SEPARATOR . $directory;

            $this->gatherFileDetails($directory);

            if (!$this->runPreObfuscationChecks()) {
                $this->logger->errorMessage(
                    'Source did not pass pre-obfuscation checks, skipping ' . $identifier
                );

                continue;
            }

            try {
                $this->emptyStorageDirs();
                $this->prepObfuscationDb();

                $this->logger->progressMessage('Processing ' . $identifier);

                $this->downloadManifestFile();
                $this->downloadSqlFiles();

                $this->importDbs();

                $this->obfuscate();

                $this->dumpDb();

                $this->pushDumpToDestination();

                $this->deleteManifest();    
            } catch (\Exception $e) {
                $this->logger->errorMessage('Could not process ' . $identifier . ' because: ' . $e->getMessage());
            }
        }

        $this->emptyStorageDirs();
    }
}


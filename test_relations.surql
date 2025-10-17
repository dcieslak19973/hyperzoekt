-- Check snapshot_file relations
SELECT * FROM snapshot_file LIMIT 10;

-- Check snapshot_commit relations  
SELECT * FROM snapshot_commit LIMIT 10;

-- Check entity_snapshot records
SELECT id, file, file_ref, sourcecontrol_commit, sourcecontrol_commit_ref FROM entity_snapshot LIMIT 5;

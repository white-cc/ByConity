#pragma once

#include <Disks/DiskByteS3.h>
#include <Storages/MergeTree/S3ObjectMetadata.h>
#include <Transaction/Actions/IAction.h>
#include <Transaction/TransactionCommon.h>

namespace DB
{
class S3AttachMetaFileAction : public IAction
{
public:
    S3AttachMetaFileAction(
        const ContextPtr & context_, const TxnTimestamp & txn_id_, const std::shared_ptr<DiskByteS3> & disk_, const String & task_id_)
        : IAction(context_, txn_id_), disk(disk_), task_id(task_id_)
    {
    }

    virtual void executeV1(TxnTimestamp commit_time) override;
    virtual void executeV2() override;
    virtual void abort() override;
    virtual void postCommit(TxnTimestamp comit_time) override;

    static void commitByUndoBuffer(const Context & ctx, const UndoResources & resources);

private:
    std::shared_ptr<DiskByteS3> disk;
    String task_id;
};

}

#pragma once

#include <Storages/MergeTree/IMergeTreeDataPart_fwd.h>
#include <Transaction/Actions/IAction.h>
#include <Transaction/TransactionCommon.h>
#include <Transaction/TxnTimestamp.h>

namespace DB
{
class S3DetachMetaAction : public IAction
{
public:
    S3DetachMetaAction(
        const ContextPtr & context_, const TxnTimestamp & txn_id_, const StoragePtr & tbl_, const MergeTreeDataPartsCNCHVector & parts_);

    virtual void executeV1(TxnTimestamp commit_time) override;
    virtual void executeV2() override;
    virtual void abort() override;
    virtual void postCommit(TxnTimestamp comit_time) override;

    static void commitByUndoBuffer(const Context & ctx, const StoragePtr & tbl, const UndoResources & resources);
    static void abortByUndoBuffer(const Context & ctx, const StoragePtr & tbl, const UndoResources & resources);

private:
    StoragePtr tbl;
    IMergeTreeDataPartsVector parts;
};

}

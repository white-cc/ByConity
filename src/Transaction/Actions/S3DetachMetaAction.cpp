#include <Catalog/Catalog.h>
#include <Interpreters/Context.h>
#include <Transaction/Actions/S3DetachMetaAction.h>
#include <CloudServices/CnchPartsHelper.h>
namespace DB
{
S3DetachMetaAction::S3DetachMetaAction(
    const ContextPtr & context_, const TxnTimestamp & txn_id_, const StoragePtr & tbl_, const MergeTreeDataPartsCNCHVector & parts_)
    : IAction(context_, txn_id_), tbl(tbl_)
{   
    parts = CnchPartsHelper::toIMergeTreeDataPartsVector(parts_);
}

void S3DetachMetaAction::executeV1(TxnTimestamp)
{
    throw Exception("Not implemented", ErrorCodes::NOT_IMPLEMENTED);
}

void S3DetachMetaAction::executeV2()
{
    global_context.getCnchCatalog()->detachAttachedParts(tbl, tbl, parts, parts);
}

void S3DetachMetaAction::abort()
{
    std::vector<String> detached_names;
    detached_names.reserve(parts.size());
    for (const auto & part : parts)
    {
        detached_names.push_back(part->info.getPartName());
    }

    global_context.getCnchCatalog()->attachDetachedParts(tbl, tbl, detached_names, parts);
}

void S3DetachMetaAction::postCommit(TxnTimestamp)
{
    // Meta already got renamed, nothing to do, skip
}

void S3DetachMetaAction::commitByUndoBuffer(const Context &, const StoragePtr &, const UndoResources &)
{
    // Transaction got execute, all meta must got renamed, nothing to do, skip
}

void S3DetachMetaAction::abortByUndoBuffer(const Context & ctx, const StoragePtr & tbl, const UndoResources & resources)
{
    std::vector<String> part_names;
    std::for_each(resources.begin(), resources.end(), [&part_names](const UndoResource & resource) {
        if (resource.type() == UndoResourceType::S3DetachPart)
        {
            part_names.push_back(resource.placeholders(0));
        }
    });

    ctx.getCnchCatalog()->attachDetachedPartsRaw(tbl, part_names);
}

}

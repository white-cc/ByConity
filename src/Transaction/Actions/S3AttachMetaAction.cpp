#include <Transaction/Actions/S3AttachMetaAction.h>
#include <Core/UUIDHelpers.h>
#include <Transaction/TransactionCommon.h>
#include <CatalogService/Catalog.h>
#include <Disks/DiskS3.h>
#include <IO/S3Common.h>
#include <Interpreters/ServerPartLog.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

S3AttachPartsInfo::S3AttachPartsInfo(const StoragePtr& from_tbl_, const IMergeTreeDataPartsVector& former_parts_,
    const MutableMergeTreeDataPartsCNCHVector& parts_):
        from_tbl(from_tbl_), former_parts(former_parts_),
        parts(parts_)
{
    if (former_parts.size() != parts.size())
    {
        throw Exception(fmt::format("Former part's size {} not equals to parts size {}",
            former_parts.size(), parts.size()), ErrorCodes::LOGICAL_ERROR);
    }
}

void S3AttachMetaAction::executeV1(TxnTimestamp )
{
    throw Exception("Not implemented", ErrorCodes::NOT_IMPLEMENTED);
}

void S3AttachMetaAction::executeV2()
{
    std::vector<String> detached_part_names;
    detached_part_names.reserve(former_parts.size());
    for (const IMergeTreeDataPartPtr& part : former_parts)
    {
        detached_part_names.push_back(part == nullptr ? "" : part->info.getPartName());
    }
    context.getCnchCatalog()->attachDetachedParts(from_tbl, to_tbl,
        detached_part_names, {parts.begin(), parts.end()});
}

void S3AttachMetaAction::abort()
{
    context.getCnchCatalog()->detachAttachedParts(to_tbl, from_tbl,
        {parts.begin(), parts.end()}, former_parts);

    ServerPartLog::addNewParts(context, ServerPartLogElement::INSERT_PART, parts,
        txn_id, true);
}

void S3AttachMetaAction::postCommit(TxnTimestamp commit_time)
{
    context.getCnchCatalog()->setCommitTime(to_tbl,
        CatalogService::CommitItems{{parts.begin(), parts.end()}, {}, {}}, commit_time);

    for (auto& part : parts)
    {
        part->commit_time = commit_time;
    }

    ServerPartLog::addNewParts(context, ServerPartLogElement::INSERT_PART, parts,
        txn_id, false);
}

void S3AttachMetaAction::collectUndoResourcesForCommit(const UndoResources& resources,
    NameSet& part_names)
{
    // Committed, only set commit time, it required part name with hint mutation
    // we store both name and name with hint mutation to avoid parse a data model again
    for (const UndoResource& resource : resources)
    {
        if (resource.type() == UndoResourceType::S3AttachPart)
        {
            part_names.insert(resource.placeholders(1));
        }
    }
}

void S3AttachMetaAction::abortByUndoBuffer(const Context& ctx, const StoragePtr& tbl,
    const UndoResources& resources)
{
    // Map key is from table's uuid
    // First element of map value is former part name and former part meta(The detached part meta)
    // Second element of map value is new part name(The attached part name)
    std::map<String, std::pair<std::vector<std::pair<String, String>>, std::vector<String>>> part_meta_mapping;
    for (const UndoResource& resource : resources)
    {
        if (resource.type() == UndoResourceType::S3AttachPart)
        {
            const String& from_tbl_uuid = resource.placeholders(0);
            // const String& former_part_name_with_hint = resource.placeholders(1);
            const String& former_part_name = resource.placeholders(2);
            const String& former_part_meta = resource.placeholders(3);
            const String& new_part_name = resource.placeholders(4);

            part_meta_mapping[from_tbl_uuid].first.emplace_back(former_part_name, former_part_meta);
            part_meta_mapping[from_tbl_uuid].second.emplace_back(new_part_name);
        }
    }

    auto catalog = ctx.getCnchCatalog();
    for (const auto& entry : part_meta_mapping)
    {
        catalog->detachAttachedPartsRaw(tbl, entry.first, entry.second.second, entry.second.first);
    }
}

}
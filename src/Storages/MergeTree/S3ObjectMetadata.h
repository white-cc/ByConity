#pragma once

#include <map>
#include <Core/Types.h>

namespace DB::S3ObjectMetadata
{

/// Mark generator of one part, since we only generate part's part at local
/// there maybe some part already have same part id in s3, so when write part
/// fail, it should have pg-id to mark the generator, when transaction rollback
/// or part writer clean up, it will use corresponding pg-id to determine if this
/// part is generate by this part_writer execution or transaction
extern const String PART_GENERATOR_ID;

/// Value of PART_GENERATOR_ID
class PartGeneratorID
{
public:
    enum Type
    {
        PART_WRITER = 0,
        TRANSACTION = 1,
        DUMPER = 2,
    };

    PartGeneratorID(Type type_, const String& id_);
    PartGeneratorID(const String& meta_);

    bool verify(const String& meta_) const;

    String str() const;

    static std::pair<Type, String> parse(const String& meta_);

    Type type;
    String id;
};

}

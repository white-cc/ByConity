#include <Disks/DiskType.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

int DiskType::toInt(Type disk_type)
{
    return static_cast<int>(disk_type);
}

DiskType::Type DiskType::toType(int disk_type_id)
{   
    if (disk_type_id > 5)
    {
        throw Exception("Unknown disk type id " + std::to_string(disk_type_id),
            ErrorCodes::BAD_ARGUMENTS);
    }
    return static_cast<Type>(disk_type_id);
}

}

from google.protobuf import descriptor
from google.protobuf import message_factory
from google.protobuf import descriptor_pb2

def test_encoding():
    # Define Map Entry
    entry_desc = descriptor.Descriptor(
        name='Entry',
        full_name='SetSessionOptionsRequest.Entry',
        filename=None,
        containing_type=None,
        fields=[
            descriptor.FieldDescriptor(
                name='key', full_name='SetSessionOptionsRequest.Entry.key', index=0, number=1,
                type=descriptor.FieldDescriptor.TYPE_STRING, cpp_type=descriptor.FieldDescriptor.CPPTYPE_STRING,
                label=descriptor.FieldDescriptor.LABEL_OPTIONAL, has_default_value=False, default_value=b"",
                containing_type=None, message_type=None, enum_type=None, containing_oneof=None,
                is_extension=False, extension_scope=None, options=None),
            descriptor.FieldDescriptor(
                name='value', full_name='SetSessionOptionsRequest.Entry.value', index=1, number=2,
                type=descriptor.FieldDescriptor.TYPE_STRING, cpp_type=descriptor.FieldDescriptor.CPPTYPE_STRING,
                label=descriptor.FieldDescriptor.LABEL_OPTIONAL, has_default_value=False, default_value=b"",
                containing_type=None, message_type=None, enum_type=None, containing_oneof=None,
                is_extension=False, extension_scope=None, options=None),
        ],
        nested_types=[], enum_types=[], extensions=[], options=None, is_extendable=False, syntax='proto3')

    # Define Request
    request_desc = descriptor.Descriptor(
        name='SetSessionOptionsRequest',
        full_name='SetSessionOptionsRequest',
        filename=None,
        containing_type=None,
        fields=[
            descriptor.FieldDescriptor(
                name='session_options', full_name='SetSessionOptionsRequest.session_options', index=0, number=1,
                type=descriptor.FieldDescriptor.TYPE_MESSAGE, cpp_type=descriptor.FieldDescriptor.CPPTYPE_MESSAGE,
                label=descriptor.FieldDescriptor.LABEL_REPEATED, has_default_value=False, default_value=None,
                containing_type=None, message_type=entry_desc, enum_type=None, containing_oneof=None,
                is_extension=False, extension_scope=None, options=None),
        ],
        nested_types=[entry_desc], enum_types=[], extensions=[], options=None, is_extendable=False, syntax='proto3')

    # Create class
    Factory = message_factory.MessageFactory()
    SetSessionOptionsRequest = Factory.GetPrototype(request_desc)

    # Instantiate and encode
    req = SetSessionOptionsRequest()
    entry = req.session_options.add()
    entry.key = "project_id"
    entry.value = "123"
    
    encoded = req.SerializeToString()
    print(f"Generated hex: {encoded.hex()}")
    
    # Compare with my manual encoding
    from dremioframe.flight_sql import encode_set_session_options
    manual = encode_set_session_options({"project_id": "123"})
    print(f"Manual hex:    {manual.hex()}")
    
    if encoded == manual:
        print("MATCH!")
    else:
        print("MISMATCH!")

if __name__ == "__main__":
    try:
        test_encoding()
    except Exception as e:
        print(f"Error: {e}")

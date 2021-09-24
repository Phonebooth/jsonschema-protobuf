var protobuf = require('protocol-buffers-schema')
const util = require('util');

var mappings = {
  'array': 'repeated',
  'object': 'message',
  'integer': 'uint32',
  'long': 'uint64',
  'number': 'int32',
  'string': 'string',
  'boolean': 'bool',
  'binary': 'bytes'
}

var importTypes = new Set();

module.exports.convert = function (schema, package) {
  if (typeof schema === 'string') {
    schema = JSON.parse(schema)
  }
  
  var result = {
    syntax: 3,
    package: package,
    imports: [],
    enums: [],
    messages: [],
    options: {},
    extends: []
  }

  if (schema.type === 'object') {
    result.messages.push(Message(schema))
  }
  
  console.log(`import types ${Array.from(importTypes)}`);
  for (type of importTypes) {
    result.imports.push(`${type}.proto`);
  }
  importTypes.clear();
  
  return protobuf.stringify(result)
}

module.exports.combine = function(imports, package) {
  var message = {
    syntax: 3,
    package: package,
    name: "",
    enums: [],
    messages: [],
    fields: [],
    imports: imports
  };
  return protobuf.stringify(message);
}

function Message (schema) {
  var message = {
    name: schema.protobuf_name,
    enums: [],
    messages: [],
    fields: []
  }

  for (var key in schema.properties) {
    var field = schema.properties[key]
    field.name = key;
    
    console.log(`processing field ${key}: type ${field.type} object_type ${field.object_type}`);
    
//    if (field.type === 'object' && !field.hasOwnProperty('object_type')) {
//      message.messages.push(Field(field))
//    }
    if (field.type === 'atom' && field.hasOwnProperty('enum')) {
        // create a field for this enum type, and create an enum struct at the top level
        message.fields.push(Field(field));        
        message.enums.push(Enum(field));
    } 
    else {
      message.fields.push(Field(field));
    }
  }

  // sort the fields by tag
  message.fields.sort((a, b) => a.tag - b.tag);

  for (var i in schema.required) {
    var required = schema.required[i]
    for (var i in message.fields) {
      var field = message.fields[i]
      if (required === field.name) 
        field.required = true
    }
  }

  return message
}

function Field (field) {
  let type = '';
  // raw maps have type object but no object_type
  if (field.type == 'object' && !field.hasOwnProperty('object_type') && field.hasOwnProperty('additionalProperties')) {
    let value = field.additionalProperties['type'];
    type = `map<string, ${value}>`;
    console.log(`${field.name} raw map type ${type}`);
  }
  // objects with an object_type are references to other types
  else if (field.type == 'object' && field.hasOwnProperty('object_type')) {
    type = field.object_type;
    let snakeCase = toSnakeCase(type);
    importTypes.add('ipersist_' + snakeCase);
    console.log(`snake case ${snakeCase}`);
    console.log(`${field.name} object reference type ${type}`);
  }
  // references to enum types use the camel case field name as the enum type
  else if (field.type === 'atom' && field.hasOwnProperty('enum')) {
    type = toCamelCase(field.name);
    console.log(`${field.name} enum type ${type}`);
  }
  // all other types use the mapping, or the defined type as default 
  else {
    type = mappings[field.type] || field.type;
    console.log(`${field.name} regular type ${type}`);
  }

  console.log(`field type will be ${type}`)
  var repeated = false
  let tag = field.protobuf_sequence_number;

  if (field.type === 'array') {
    repeated = true
    type = field.items.type == "object" ? field.items.object_type : field.items.type;
    if (field.items.hasOwnProperty('object_type')) {
        let snakeCase = toSnakeCase(field.items.object_type);
        importTypes.add('ipersist_' + snakeCase);
    }
  }

  return {
    name: field.name,
    type: type,
    tag: tag,
    repeated: repeated
  }
}

function Enum(field) {
  let valuesMap = {};
  field.enum.forEach((elem, index) => valuesMap[elem] = index);
  var e = {
    name: toCamelCase(field.name),
    values: valuesMap
  }
  return e;
}


function toSnakeCase(string) {
    return string.replace(/[A-Z]/g, (letter, index) => (index == 0 ? letter.toLowerCase() : '_'+ letter.toLowerCase()) );
}

function toCamelCase(string) {
    let matches = string.split('_');//string.match(/([^_]+)/g);
    let out = "";
    for (match of matches) {
        out += match.charAt(0).toUpperCase() + match.slice(1);
    }
    return out;
}


// old version with proto file parsing below

//var protobuf = require('protocol-buffers-schema')
//const util = require('util');
//
//var mappings = {
//  'array': 'repeated',
//  'object': 'message',
//  'integer': 'int32',
//  'number': 'int32',
//  'string': 'string',
//  'boolean': 'bool'
//}
//
//module.exports = function (schema, pb) {
//  if (typeof schema === 'string') {
//    schema = JSON.parse(schema)
//  }
//  if (typeof pb === 'string') {
//    pb = protobuf.parse(pb);
//  }
//  
//  var result = {
//    syntax: 3,
//    package: null,
//    enums: [],
//    messages: []
//  }
//
//  if (schema.type === 'object') {
//    result.messages.push(Message(schema, pb))
//  }
//  return protobuf.stringify(result)
//}
//
//function Message (schema, pb) {
//  var message = {
//    name: schema.name,
//    enums: [],
//    messages: [],
//    fields: []
//  }
//
//  // find the message inside pb whose name matches schema.name, use it for comparison
//  let pbMsg = findMatchingMessage(pb, schema.name);
//
//  var tag = 1
//  for (var key in schema.properties) {
//    var field = schema.properties[key]
//    if (field.type === 'object') {
//      field.name = key
//      message.messages.push(Message(field, pbMsg))
//    } else {
//      field.name = key;
//      let newField = Field(field, tag, pbMsg);
//      message.fields.push(newField)
//      tag = newField.tag + 1;
//    }
//  }
//
//  for (var i in schema.required) {
//    var required = schema.required[i]
//    for (var i in message.fields) {
//      var field = message.fields[i]
//      if (required === field.name) 
//        field.required = true
//    }
//  }
//
//  return message
//}
//
//function Field (field, tag, pb) {
//  var type = mappings[field.type] || field.type
//  var repeated = false
//
//  if (field.type === 'array') {
//    repeated = true
//    type = field.items.type
//  }
//
//  let pbField = findMatchingField(pb, field.name);
//
//  // use the pb field's tag if it exists
//  let newTag = pbField?.tag ?? tag;
//
//  if (newTag < tag) {
//      console.error(`something has gone wrong, field ${field.name} has tag ${newTag}, but tag sequence is ${tag}. Was a new field inserted in the fields?`);
//  }
//
//  return {
//    name: field.name,
//    type: type,
//    tag: newTag,
//    repeated: repeated
//  }
//}
//
//
//function findMatchingMessage(pb, name) {
//    if (!pb) {
//        console.log(`pb is empty for name ${name}`);
//        return null;
//    }
//    for (msg of pb.messages) {
//        if (msg.name == name) {
//            console.log(`found matching msg for: ${name}`);
//            return msg;
//        }
//    }
//    console.error(`couldn't find pb msg for ${name}`);
//    console.error(`searched pb ${util.inspect(pb, {showHidden: false, depth: null, colors: true})}`);
//    return null;
//}
//
//function findMatchingField(pb, name) {
//    if (!pb) {
//        console.log(`pb is empty for name ${name}`);
//        return null;
//    }
//    for (field of pb.fields) {
//        if (field.name == name) {
//            console.log(`found matching field for: ${name}`);
//            return field;
//        }
//    }
//    console.error(`couldn't find pb field for ${name}`);
//    console.error(`searched pb ${util.inspect(pb, {showHidden: false, depth: null, colors: true})}`);
//    return null;
//}

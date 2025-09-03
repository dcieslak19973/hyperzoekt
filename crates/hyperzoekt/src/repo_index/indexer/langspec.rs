// Copyright 2025 HyperZoekt Project
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// langspecs are simple descriptors; no external imports required here

pub trait LangSpec {
    fn class_kind(&self) -> &'static str;
    fn class_name_field(&self) -> &'static str;
    fn function_kind(&self) -> &'static str;
    fn function_name_field(&self) -> &'static str;
}

macro_rules! make_spec {
    ($name:ident, $class:expr, $class_field:expr, $func:expr, $func_field:expr) => {
        pub struct $name;
        impl LangSpec for $name {
            fn class_kind(&self) -> &'static str {
                $class
            }
            fn class_name_field(&self) -> &'static str {
                $class_field
            }
            fn function_kind(&self) -> &'static str {
                $func
            }
            fn function_name_field(&self) -> &'static str {
                $func_field
            }
        }
    };
}

make_spec!(RustLangSpec, "struct_item", "name", "function_item", "name");
make_spec!(
    PythonLangSpec,
    "class_definition",
    "name",
    "function_definition",
    "name"
);
make_spec!(
    JsLangSpec,
    "class_declaration",
    "name",
    "function_declaration",
    "name"
);
make_spec!(
    TsLangSpec,
    "class_declaration",
    "name",
    "function_declaration",
    "name"
);
make_spec!(
    JavaLangSpec,
    "class_declaration",
    "name",
    "method_declaration",
    "name"
);
make_spec!(
    GoLangSpec,
    "type_declaration",
    "name",
    "function_declaration",
    "name"
);
make_spec!(
    CppLangSpec,
    "class_specifier",
    "name",
    "function_definition",
    "declarator"
);
make_spec!(
    CSharpLangSpec,
    "class_declaration",
    "name",
    "method_declaration",
    "name"
);
make_spec!(
    SwiftLangSpec,
    "class_declaration",
    "name",
    "function_declaration",
    "name"
);
make_spec!(
    VerilogLangSpec,
    "module_declaration",
    "name",
    "function_declaration",
    "name"
);

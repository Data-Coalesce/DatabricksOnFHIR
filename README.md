# DatabricksOnFHIR
mermaid
graph LR
A[(Bronze Data)] -- Select Claims -->df[[Dataframe]] 
subgraph "Notebook Step 1 (Python)"
df --> B["💥Explode Array of Structs"]
B --> C(Encounter Struct)
B --> D(Claim Item Struct)
B --> E(Procedure Struct)
df --> F(("🧹Cleanse"))
D --> G(("🧹Cleanse"))
G --> H{{"🧩Restruct"}}
end
subgraph "Notebook Step 2 (Python)"
grp{Regroup}
agg{{Aggregate}}
grp --> agg
F --> grp
C --> I
E --> I
H --> I((Collect Sets))
I --> agg
end
write[(Silver Data)]
agg --> write


const raw_dataset = dataform.projectConfig.vars.raw_dataset;

["restaurants", "customers", "orders", "order_items"].forEach((table) => {
  declare({
    database: dataform.projectConfig.defaultProject,
    schema: raw_dataset,
    name: table,
  });
});
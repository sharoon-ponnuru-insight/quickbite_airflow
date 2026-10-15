const raw_dataset = dataform.projectConfig.vars.raw_dataset;

["raw_restaurants", "raw_customers", "raw_orders", "raw_order_items"].forEach((table) => {
  declare({
    schema: raw_dataset,
    name: table,
  });
});
export const ORDER_CSV_COLUMNS = [
  "time",
  "broker",
  "order_id",
  "client_order_id",
  "deployment",
  "symbol",
  "side",
  "quantity",
  "executed_quantity",
  "state",
  "reason",
];

export function buildOrderCsvRows(rows = [], deploymentId = "", formatTime = () => "") {
  return (rows || []).map((row) => {
    const order = row?.order && typeof row.order === "object" ? row.order : row || {};
    return {
      time: formatTime(order),
      broker: order.broker_id || "",
      order_id: order.order_id || "",
      client_order_id: order.client_order_id || order.idempotency_key || "",
      deployment: order.deployment_id || deploymentId || "",
      symbol: order.symbol || "",
      side: order.side || "",
      quantity: order.quantity ?? order.qty ?? "",
      executed_quantity: order.executed_quantity ?? order.executed_volume ?? "",
      state: order.state || "",
      reason: order.reason || "",
    };
  });
}

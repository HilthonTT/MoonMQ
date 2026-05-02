local TopicManager = require("src.topic_manager")

local tm = TopicManager.new("./data")

local topic, err = tm:create_topic("orders", 3)
if err or not topic then
    print("error:", err)
    os.exit(1)
end

print("Started topic:", topic.name, "with", #topic.partitions, "partitions")

-- main loop
while true do
    for _, p in ipairs(topic.partitions) do
        p:tick()
    end
end

function filter_khc_yarn_log(tag, timestamp, record)
    
    local path = record["file_path"]
    local applicationid = string.match(path, "application_%d+_%d+")
    
    if applicationid then
        record["applicationid"] = applicationid
    end
    
    local log_message = record["log"]

    -- Step 정보 추출 (예시)
    -- 로그 메시지 필터링
    --if string.match(log_message, "^%d%d/%d%d/%d%d %d%d:%d%d:%d%d (ERROR|INFO KHCLogger: Step %d+: %d+:%d+:%d+)") then
    if string.match(log_message, "^%d%d/%d%d/%d%d %d%d:%d%d:%d%d INFO KHCLogger: Step %d+: %d+:%d+:%d+") then
        return 1, timestamp, record
    elseif string.match(log_message, "^%d%d/%d%d/%d%d %d%d:%d%d:%d%d ERROR") then
        return 1, timestamp, record
    else
        return -1, timestamp, record
    end

end
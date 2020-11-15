package personal.leo.debezium_to_kudu.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import personal.leo.debezium_to_kudu.mapper.HistoryMapper;

@RestController
@RequestMapping("test")
public class TestController {
    @Autowired
    HistoryMapper historyMapper;

    @GetMapping("2")
    public int test2() {
        final int ss = historyMapper.count("ss");
        return ss;
    }
}

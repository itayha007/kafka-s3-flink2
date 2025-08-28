package org.example.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.example.service.refresh.RefreshService;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Log4j2
@RestController
@RequestMapping("refresh")
@RequiredArgsConstructor
public class RefreshController {
    public RefreshService refreshService;

    @PostMapping("/trigger")
    public void trigger() throws Exception {
        this.refreshService.refresh();
    }
}

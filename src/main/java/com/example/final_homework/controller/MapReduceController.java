package com.example.final_homework.controller;

import com.example.final_homework.pojo.bo.req.Request;
import com.example.final_homework.service.IHdfsService;
import com.example.final_homework.service.MapReduceService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author MrCh1ppy
 */
@Slf4j
@RestController
@RequestMapping("/api/v1/map-reduce")
@RequiredArgsConstructor
public class MapReduceController {
	private final MapReduceService mapReduceService;
	private final IHdfsService hdfsService;
	private static final String INPUT="/2020031137_training.txt";
	@PostMapping("/word/count")
	public String wordCount(@RequestBody Request request){
		if (hdfsService.existFile(INPUT)){
			hdfsService.deleteFile(INPUT);
		}
		hdfsService.uploadFile("D:\\CODE\\Java_temp\\final_homework\\src\\main\\resources\\static\\2020031137_training.txt",INPUT);
		String jobName= "check";
		String outputPath= request.getOutputPath();
		try {
			mapReduceService.wordCount(jobName, INPUT, outputPath);
			return "OK";
		} catch (Exception e) {
			log.error(e.getMessage());
			return e.getMessage();
		}
	}
}

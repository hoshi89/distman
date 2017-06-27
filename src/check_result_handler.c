check_result *copy_check_result(check_result *src) {
    check_result *cr = nm_calloc(1, sizeof(*cr));
    init_check_result(cr);
    cr->object_check_type = src->object_check_type;
    cr->host_name = src->host_name ? nm_strdup(src->host_name) : NULL;
    cr->service_description = src->service_description ? nm_strdup(src->service_description) : NULL;
    cr->check_type = src->check_type;
    cr->check_options = src->check_options;
    cr->scheduled_check = src->scheduled_check;
    cr->output_file = src->output_file ? nm_strdup(src->output_file) : NULL;
    cr->output_file_fp = src->output_file_fp;
    cr->latency = src->latency;
    cr->start_time = src->start_time;
    cr->finish_time = src->finish_time;
    cr->early_timeout = src->early_timeout;
    cr->exited_ok = src->exited_ok;
    cr->return_code = src->return_code;
    cr->output = src->output ? nm_strdup(src->output) : NULL;
    return cr;
}

static void handle_host_check_work(wproc_result *wpres, void *arg, int flags) {
	check_result *cr = (check_result *)arg;
	struct host *hst;

	/* decrement the number of host checks still out there... */
	if (currently_running_host_checks > 0)
		currently_running_host_checks--;

	hst = find_host(cr->host_name);
	if (hst && wpres) {
		hst->is_executing = FALSE;
		memcpy(&cr->rusage, &wpres->rusage, sizeof(wpres->rusage));
		cr->start_time.tv_sec = wpres->start.tv_sec;
		cr->start_time.tv_usec = wpres->start.tv_usec;
		cr->finish_time.tv_sec = wpres->stop.tv_sec;
		cr->finish_time.tv_usec = wpres->stop.tv_usec;
		if (WIFEXITED(wpres->wait_status)) {
			cr->return_code = WEXITSTATUS(wpres->wait_status);
		} else {
			cr->return_code = STATE_UNKNOWN;
		}

		if (wpres->outstd && *wpres->outstd) {
			cr->output = nm_strdup(wpres->outstd);
		} else if (wpres->outerr && *wpres->outerr) {
			nm_asprintf(&cr->output, "(No output on stdout) stderr: %s", wpres->outerr);
		} else {
			cr->output = NULL;
		}

		cr->early_timeout = wpres->early_timeout;
		cr->exited_ok = wpres->exited_ok;
		cr->engine = NULL;
		cr->source = wpres->source;

        /*
         * Put check result on the results queue
         */
         nm_log(NSLOG_INFO_MESSAGE, "DistMan: Handling host check %s", cr->host_name);

         process_check_result(cr);
	}
	free_check_result(cr);
	free(cr);
}

static void handle_service_check_work(wproc_result *wpres, void *arg, int flags) {
	check_result *cr = (check_result *)arg;
	if(wpres) {
		memcpy(&cr->rusage, &wpres->rusage, sizeof(wpres->rusage));
		cr->start_time.tv_sec = wpres->start.tv_sec;
		cr->start_time.tv_usec = wpres->start.tv_usec;
		cr->finish_time.tv_sec = wpres->stop.tv_sec;
		cr->finish_time.tv_usec = wpres->stop.tv_usec;
		if (WIFEXITED(wpres->wait_status)) {
			cr->return_code = WEXITSTATUS(wpres->wait_status);
		} else {
			cr->return_code = STATE_UNKNOWN;
		}

		if (wpres->outstd && *wpres->outstd) {
			cr->output = nm_strdup(wpres->outstd);
		} else if (wpres->outerr && *wpres->outerr) {
			nm_asprintf(&cr->output, "(No output on stdout) stderr: %s", wpres->outerr);
		} else {
			cr->output = NULL;
		}

		cr->early_timeout = wpres->early_timeout;
		cr->exited_ok = wpres->exited_ok;
		cr->engine = NULL;
		cr->source = wpres->source;

        /*
         * Put the check result on the results queue
         */
         nm_log(NSLOG_INFO_MESSAGE, "DistMan: Handling service check %s", cr->host_name);

		process_check_result(cr);
	}
	free_check_result(cr);
	free(cr);
}
